"""
Gold job: hourly trips analytics.

Что делает:
1. Читает Silver layer для конкретного года и месяца.
2. Строит hourly gold mart по:
   - pickup_date;
   - pickup_hour;
   - trip_type.
3. Считает метрики:
   - trips_count;
   - total_revenue;
   - avg_check;
   - avg_trip_distance;
   - avg_trip_duration_minutes.
4. Записывает результат в Gold layer.

Оптимизационные решения:
1. Убрали silver_df.count().
   Почему:
   - count() — отдельный Spark action;
   - он полностью сканирует Silver parquet;
   - затем write() снова читает Silver parquet для агрегации;
   - проверка, что Silver не пустой, уже есть в check_yellow_taxi_quality.py.

2. Убрали gold_df.show().
   Почему:
   - show() тоже Spark action;
   - он заставляет Spark посчитать gold_df до записи;
   - затем write() считает gold_df повторно;
   - preview полезен для debug, но не нужен в production-like pipeline.

3. Убрали orderBy(...) перед записью.
   Почему:
   - Parquet не требует сортировки строк при записи;
   - порядок строк не является надёжным контрактом parquet dataset;
   - сортировку лучше делать при чтении, в SQL, ClickHouse или Superset;
   - orderBy может добавлять лишний sort/shuffle.

4. Не используем cache/persist.
   Почему:
   - после удаления count() и show() остаётся один основной action: write();
   - silver_df читается один раз;
   - кэширование здесь только добавило бы лишнее давление на память/диск.

5. Добавили try/finally.
   Почему:
   - SparkSession должен закрываться даже если чтение, агрегация или запись упадут.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, round, sum

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_hourly_trips_path,
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
        .appName("nyc_taxi_gold_hourly_trips")
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


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    try:
        silver_path = silver_yellow_path(year, month)
        gold_path = gold_hourly_trips_path(year, month)

        print(f"Reading silver data from: {silver_path}")

        silver_df = spark.read.parquet(silver_path)

        # ВАЖНО:
        # Не делаем silver_df.count().
        #
        # Почему:
        # - это отдельный Spark action;
        # - он полностью читает Silver parquet;
        # - затем write() ниже снова читает Silver parquet, чтобы построить gold mart.
        #
        # Проверка, что Silver не пустой, уже выполняется в:
        # jobs/check_yellow_taxi_quality.py

        gold_df = (
            silver_df
            .groupBy("pickup_date", "pickup_hour", "trip_type")
            .agg(
                count("*").alias("trips_count"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("total_amount"), 2).alias("avg_check"),
                round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                round(avg("trip_duration_minutes"), 2).alias(
                    "avg_trip_duration_minutes"
                ),
            )
            .withColumn("year", col("pickup_date").substr(1, 4))
            .withColumn("month", col("pickup_date").substr(6, 2))
            .withColumn("gold_load_timestamp", current_timestamp())
        )

        # ВАЖНО:
        # Не делаем gold_df.show().
        #
        # Почему:
        # - show() запускает Spark action;
        # - затем write() запускает ещё один Spark action;
        # - без cache/persist это может привести к повторному пересчёту gold_df
        #   и повторному чтению Silver.
        #
        # Если нужен preview, лучше проверить результат после записи:
        # spark.read.parquet(gold_path).show(...)
        # или через ClickHouse/Superset после загрузки.

        # ВАЖНО:
        # Не делаем orderBy(...) перед записью.
        #
        # Почему:
        # - для parquet dataset порядок строк не является бизнес-контрактом;
        # - сортировка должна выполняться в запросах/BI;
        # - orderBy может добавить лишний sort/shuffle.
        #
        # Если нужен отсортированный вывод, используем ORDER BY на уровне SQL.

        print(f"Writing gold data to: {gold_path}")

        (
            gold_df
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )

        print("Gold hourly trips job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)