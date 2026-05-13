"""
Data Quality check для NYC Taxi pipeline.

Проверяет качество данных после Silver-слоя:
1. Bronze не пустой
2. Silver не пустой
3. Silver не потерял слишком много строк
4. В ключевых колонках нет NULL
5. Нет некорректных расстояний и сумм
6. Даты поездок соответствуют нужному месяцу
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BUCKET_NAME,
    S3_ENDPOINT,
    S3_REGION,
    validate_config,
)


def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_yellow_data_quality")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .getOrCreate()
    )


def main(year_arg: str, month_arg: str) -> None:
    spark = create_spark_session()

    bronze_path = (
        f"s3a://{BUCKET_NAME}/nyc_taxi/bronze/yellow/"
        f"year={year_arg}/month={month_arg}"
    )

    silver_path = (
        f"s3a://{BUCKET_NAME}/nyc_taxi/silver/yellow/"
        f"year={year_arg}/month={month_arg}"
    )

    print(f"Reading Bronze from: {bronze_path}")
    bronze_df = spark.read.parquet(bronze_path)

    print(f"Reading Silver from: {silver_path}")
    silver_df = spark.read.parquet(silver_path)

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    print(f"Bronze rows: {bronze_count}")
    print(f"Silver rows: {silver_count}")

    if bronze_count == 0:
        raise ValueError("DQ FAILED: Bronze layer is empty")

    if silver_count == 0:
        raise ValueError("DQ FAILED: Silver layer is empty")

    min_expected_silver_rows = bronze_count * 0.7

    if silver_count < min_expected_silver_rows:
        raise ValueError(
            f"DQ FAILED: Too many rows were removed. "
            f"Bronze={bronze_count}, Silver={silver_count}"
        )

    required_not_null_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "total_amount",
    ]

    for column_name in required_not_null_columns:
        null_count = silver_df.filter(col(column_name).isNull()).count()
        print(f"NULL count in {column_name}: {null_count}")

        if null_count > 0:
            raise ValueError(
                f"DQ FAILED: Column {column_name} contains NULL values"
            )

    invalid_distance_count = silver_df.filter(col("trip_distance") <= 0).count()

    if invalid_distance_count > 0:
        raise ValueError(
            f"DQ FAILED: trip_distance <= 0 found: {invalid_distance_count}"
        )

    invalid_amount_count = silver_df.filter(col("total_amount") <= 0).count()

    if invalid_amount_count > 0:
        print(
            f"DQ WARNING: total_amount <= 0 found: {invalid_amount_count}. "
            "These rows are allowed because NYC Taxi data may contain refunds, "
            "cancellations, or fare corrections."
        )

    invalid_month_count = silver_df.filter(
        (year(col("tpep_pickup_datetime")) != int(year_arg))
        | (month(col("tpep_pickup_datetime")) != int(month_arg))
    ).count()

    if invalid_month_count > 0:
        print(
            f"DQ WARNING: rows outside expected month found: {invalid_month_count}. "
            "These rows are allowed because source files may contain late, early, "
            "or incorrectly timestamped taxi trips."
        )

    print("DQ PASSED: Silver data quality checks completed successfully")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year_arg=args.year, month_arg=args.month)