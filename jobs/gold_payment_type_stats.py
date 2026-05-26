"""
Gold job: payment type statistics.

Что делает:
1. Читает Silver layer для конкретного года и месяца.
2. Добавляет человекочитаемое название payment_type.
3. Строит payment-type gold mart по:
   - pickup_date;
   - trip_type;
   - payment_type;
   - payment_type_name.
4. Считает payment metrics:
   - trips_count;
   - total_revenue;
   - avg_check;
   - total_tips;
   - avg_tip;
   - tips_share_from_revenue.
5. Записывает результат в Gold layer.

Оптимизационные решения:
1. Убрали silver_df.count().
   Почему:
   - count() — отдельный Spark action;
   - он полностью сканирует Silver parquet;
   - затем write() снова читает Silver parquet для построения gold mart;
   - проверка, что Silver не пустой, уже выполняется в check_yellow_taxi_quality.py.

2. Убрали gold_df.show().
   Почему:
   - show() тоже Spark action;
   - он заставляет Spark посчитать gold_df до записи;
   - затем write() считает gold_df повторно;
   - preview полезен для debug, но не нужен в production-like pipeline.

3. Убрали orderBy(...) перед записью.
   Почему:
   - Parquet dataset не гарантирует порядок строк при последующем чтении;
   - сортировка должна выполняться в SQL, ClickHouse или Superset;
   - orderBy может добавлять лишний sort/shuffle.

4. tips_share_from_revenue считаем после агрегации.
   Почему:
   - сначала считаем raw суммы total_revenue_raw и total_tips_raw;
   - потом считаем ratio через уже готовые aggregated columns;
   - это делает формулу понятнее;
   - дополнительно защищаемся от деления на 0.

5. Не используем cache/persist.
   Почему:
   - после удаления count() и show() остаётся один основной action: write();
   - silver_df не используется несколькими независимыми actions;
   - cache/persist здесь только добавил бы лишнее давление на память/диск.

6. Добавили try/finally.
   Почему:
   - SparkSession должен закрываться даже если чтение, агрегация или запись упадут.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    round,
    sum,
    lit,
    when,
)

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_payment_type_stats_path,
    silver_yellow_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    validate_config() проверяет обязательные переменные окружения до старта Spark job.
    """

    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_gold_payment_type_stats")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def build_gold_payment_type_stats(silver_df: DataFrame) -> DataFrame:
    """
    Build the payment type Gold mart from the Silver dataframe.

    The output is aggregated by:
    - pickup_date;
    - trip_type;
    - payment_type;
    - payment_type_name.

    Important:
    This function preserves the optimized logic from the original job:
    - first calculate raw aggregate values;
    - then round final output columns;
    - calculate tips_share_from_revenue from raw aggregated sums;
    - protect against division by zero.

    The function does not read or write data, so it can be tested with a small
    in-memory Spark DataFrame.
    """

    enriched_df = silver_df.withColumn(
        "payment_type_name",
        when(col("payment_type") == 1, lit("Credit card"))
        .when(col("payment_type") == 2, lit("Cash"))
        .when(col("payment_type") == 3, lit("No charge"))
        .when(col("payment_type") == 4, lit("Dispute"))
        .when(col("payment_type") == 5, lit("Unknown"))
        .when(col("payment_type") == 6, lit("Voided trip"))
        .otherwise(lit("Other")),
    )

    aggregated_df = (
        enriched_df
        .groupBy("pickup_date", "trip_type", "payment_type", "payment_type_name")
        .agg(
            count("*").alias("trips_count"),
            sum("total_amount").alias("total_revenue_raw"),
            avg("total_amount").alias("avg_check_raw"),
            sum("tip_amount").alias("total_tips_raw"),
            avg("tip_amount").alias("avg_tip_raw"),
        )
    )

    return (
        aggregated_df
        .withColumn("total_revenue", round(col("total_revenue_raw"), 2))
        .withColumn("avg_check", round(col("avg_check_raw"), 2))
        .withColumn("total_tips", round(col("total_tips_raw"), 2))
        .withColumn("avg_tip", round(col("avg_tip_raw"), 2))
        .withColumn(
            "tips_share_from_revenue",
            when(
                col("total_revenue_raw") != 0,
                round(col("total_tips_raw") / col("total_revenue_raw"), 4),
            ).otherwise(lit(0.0)),
        )
        .withColumn("year", col("pickup_date").substr(1, 4))
        .withColumn("month", col("pickup_date").substr(6, 2))
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "pickup_date",
            "trip_type",
            "payment_type",
            "payment_type_name",
            "trips_count",
            "total_revenue",
            "avg_check",
            "total_tips",
            "avg_tip",
            "tips_share_from_revenue",
            "year",
            "month",
            "gold_load_timestamp",
        )
    )


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    try:
        silver_path = silver_yellow_path(year, month)
        gold_path = gold_payment_type_stats_path(year, month)

        print(f"Reading silver data from: {silver_path}")

        silver_df = spark.read.parquet(silver_path)

        # ВАЖНО:
        # Не делаем silver_df.count().
        #
        # Почему:
        # - count() запускает отдельный Spark action;
        # - Spark полностью читает Silver parquet;
        # - затем write() ниже снова читает Silver parquet для построения gold mart.
        #
        # Проверка, что Silver не пустой, уже выполняется в:
        # jobs/check_yellow_taxi_quality.py

        # Добавляем человекочитаемое название способа оплаты.
        #
        # Это небольшая transformation, она lazy:
        # физически Spark ничего не считает здесь до action write().

        gold_df = build_gold_payment_type_stats(silver_df)

        # ВАЖНО:
        # Не делаем gold_df.show().
        #
        # Почему:
        # - show() запускает Spark action;
        # - затем write() запускает ещё один Spark action;
        # - без cache/persist это может повторно пересчитать gold_df.
        #
        # Если нужен preview, лучше проверять результат после записи:
        # spark.read.parquet(gold_path).show(...)
        # или через ClickHouse/Superset после загрузки.

        # ВАЖНО:
        # Не делаем orderBy(...) перед записью.
        #
        # Почему:
        # - Parquet хранится как набор part-файлов;
        # - порядок строк не является надёжным контрактом хранения;
        # - сортировку лучше делать на уровне SQL/BI;
        # - orderBy может добавить лишний sort/shuffle.

        print(f"Writing gold data to: {gold_path}")

        (
            gold_df
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )

        print("Gold payment type stats job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)