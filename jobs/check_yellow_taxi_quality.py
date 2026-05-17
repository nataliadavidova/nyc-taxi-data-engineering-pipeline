"""
Data Quality check для NYC Taxi pipeline.

Проверяет качество данных после Silver-слоя:
1. Bronze не пустой.
2. Silver не пустой.
3. Silver не потерял слишком много строк.
4. В ключевых колонках нет NULL.
5. Нет некорректных расстояний.
6. Даты поездок соответствуют нужному месяцу.
7. payment_type заполнен и входит в ожидаемый диапазон.
8. PULocationID и DOLocationID заполнены и положительные.
9. pickup_hour заполнен и находится в диапазоне 0–23.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    bronze_yellow_path,
    silver_yellow_path,
    validate_config,
    get_month_boundaries,
)

VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]

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

    bronze_path = bronze_yellow_path(year_arg, month_arg)
    silver_path = silver_yellow_path(year_arg, month_arg)

    print(f"Reading Bronze from: {bronze_path}")
    bronze_df = spark.read.parquet(bronze_path)

    print(f"Reading Silver from: {silver_path}")
    silver_df = spark.read.parquet(silver_path)

    month_start, next_month_start = get_month_boundaries(year_arg, month_arg)

    outside_month_count = silver_df.filter(
        (col("pickup_date") < lit(month_start).cast("date"))
        | (col("pickup_date") >= lit(next_month_start).cast("date"))
    ).count()

    print(f"Silver rows outside expected month: {outside_month_count}")

    if outside_month_count > 0:
        raise ValueError(
            f"Silver contains {outside_month_count} rows outside "
            f"expected pickup date range [{month_start}, {next_month_start})"
        )

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
        "pickup_date",
        "pickup_hour",
        "PULocationID",
        "DOLocationID",
        "payment_type",
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

    invalid_payment_type_count = silver_df.filter(
        ~col("payment_type").isin(VALID_PAYMENT_TYPES)
    ).count()

    print(f"Invalid payment_type count: {invalid_payment_type_count}")

    if invalid_payment_type_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid payment_type found: {invalid_payment_type_count}"
        )

    invalid_pickup_location_count = silver_df.filter(
        col("PULocationID") <= 0
    ).count()

    print(f"Invalid PULocationID count: {invalid_pickup_location_count}")

    if invalid_pickup_location_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid PULocationID found: {invalid_pickup_location_count}"
        )

    invalid_dropoff_location_count = silver_df.filter(
        col("DOLocationID") <= 0
    ).count()

    print(f"Invalid DOLocationID count: {invalid_dropoff_location_count}")

    if invalid_dropoff_location_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid DOLocationID found: {invalid_dropoff_location_count}"
        )

    invalid_pickup_hour_count = silver_df.filter(
        (col("pickup_hour") < 0) | (col("pickup_hour") > 23)
    ).count()

    print(f"Invalid pickup_hour count: {invalid_pickup_hour_count}")

    if invalid_pickup_hour_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid pickup_hour found: {invalid_pickup_hour_count}"
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

    print("DQ PASSED: Silver data quality checks completed successfully")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year_arg=args.year, month_arg=args.month)