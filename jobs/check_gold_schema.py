"""
Check schemas and sample rows for NYC Taxi gold marts.

Что делает:
1. Читает gold parquet-витрины из Object Storage.
2. Печатает schema для каждой витрины.
3. Показывает несколько строк для быстрой проверки.
4. Поддерживает запуск по конкретному году и месяцу.

Как запускать:
python jobs/check_gold_schema.py --year 2024 --month 01
"""

import argparse

from pyspark.sql import SparkSession

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_daily_trips_path,
    gold_hourly_trips_path,
    gold_location_pair_stats_path,
    gold_payment_type_stats_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("check_gold_schema")
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


def check_gold_table(spark: SparkSession, table_name: str, path: str) -> None:
    print("=" * 80)
    print(f"Checking gold table: {table_name}")
    print(f"Path: {path}")

    df = spark.read.parquet(path)

    print("Schema:")
    df.printSchema()

    rows_count = df.count()
    print(f"Rows count: {rows_count}")

    print("Sample rows:")
    df.show(5, truncate=False)


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    gold_tables = {
        "gold_daily_trips": gold_daily_trips_path(year, month),
        "gold_hourly_trips": gold_hourly_trips_path(year, month),
        "gold_location_pair_stats": gold_location_pair_stats_path(year, month),
        "gold_payment_type_stats": gold_payment_type_stats_path(year, month),
    }

    for table_name, path in gold_tables.items():
        check_gold_table(spark, table_name, path)

    spark.stop()

    print("Gold schema check completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)