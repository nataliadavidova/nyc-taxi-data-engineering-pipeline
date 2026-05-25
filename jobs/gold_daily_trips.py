"""
Gold job: daily trips analytics.

Что делает:
1. Читает Silver layer для конкретного года и месяца.
2. Агрегирует данные до дневного уровня.
3. Считает daily taxi metrics:
   - trips_count;
   - total_revenue;
   - avg_check;
   - avg_trip_distance;
   - avg_trip_duration_minutes;
   - short_trips_count;
   - medium_trips_count;
   - long_trips_count.
4. Записывает результат в Gold layer.

Оптимизационные решения:
1. Убрали silver_df.count().
   Почему:
   - count() — отдельный Spark action;
   - он заставляет Spark полностью читать Silver parquet;
   - затем write() снова читает Silver parquet;
   - проверка, что Silver не пустой, уже есть в check_yellow_taxi_quality.py.

2. Убрали gold_df.show().
   Почему:
   - show() тоже Spark action;
   - он пересчитывает gold_df до записи;
   - затем write() пересчитывает gold_df повторно;
   - preview полезен для debug, но не нужен в production-like pipeline.

3. Убрали orderBy("pickup_date") перед записью.
   Почему:
   - Parquet не требует сортировки строк при записи;
   - порядок строк лучше задавать при чтении/аналитическом запросе;
   - orderBy может добавлять лишний sort/shuffle.

4. Добавили try/finally.
   Почему:
   - SparkSession должен закрываться даже если чтение, агрегация или запись упадут.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, round, sum, when

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_daily_trips_path,
    silver_yellow_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_gold_daily_trips")
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
        gold_path = gold_daily_trips_path(year, month)

        print(f"Reading silver data from: {silver_path}")

        silver_df = spark.read.parquet(silver_path)

        # ВАЖНО:
        # Не делаем silver_df.count() здесь.
        #
        # Почему:
        # - это отдельный Spark action;
        # - он полностью сканирует Silver parquet;
        # - затем write() ниже снова читает эти же данные.
        #
        # Проверка, что Silver не пустой, уже выполняется отдельным DQ job:
        # jobs/check_yellow_taxi_quality.py

        gold_df = (
            silver_df
            .groupBy("pickup_date")
            .agg(
                count("*").alias("trips_count"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("total_amount"), 2).alias("avg_check"),
                round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                round(avg("trip_duration_minutes"), 2).alias(
                    "avg_trip_duration_minutes"
                ),
                sum(
                    when(col("trip_type") == "short", 1).otherwise(0)
                ).alias("short_trips_count"),
                sum(
                    when(col("trip_type") == "medium", 1).otherwise(0)
                ).alias("medium_trips_count"),
                sum(
                    when(col("trip_type") == "long", 1).otherwise(0)
                ).alias("long_trips_count"),
            )
            .withColumn("year", col("pickup_date").substr(1, 4))
            .withColumn("month", col("pickup_date").substr(6, 2))
            .withColumn("gold_load_timestamp", current_timestamp())
        )

        # ВАЖНО:
        # Не делаем gold_df.show() в production-like job.
        #
        # Почему:
        # - show() запускает Spark action;
        # - затем write() запускает ещё один Spark action;
        # - это может привести к повторному чтению Silver parquet.
        #
        # Если нужен preview, лучше смотреть результат отдельным read/query
        # после записи или через ClickHouse/Superset.

        print(f"Writing gold data to: {gold_path}")

        (
            gold_df
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )

        print("Gold daily trips job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)