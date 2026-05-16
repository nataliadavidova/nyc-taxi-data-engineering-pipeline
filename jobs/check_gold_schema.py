"""
Check schemas and data quality for NYC Taxi gold marts in Object Storage.

What this job does:
1. Reads gold parquet marts from S3-compatible Object Storage.
2. Validates expected columns for each gold mart.
3. Validates that each gold mart is not empty.
4. Validates that pickup_date values belong to the expected processing month.
5. Validates mart-specific fields:
   - pickup_hour is between 0 and 23 for hourly trips;
   - payment_type_name is not empty for payment type stats;
   - pickup_zone and dropoff_zone are not empty for location pair stats.
6. Prints schemas and sample rows for observability.

How to run:
spark-submit jobs/check_gold_schema.py --year 2024 --month 01
"""

import argparse
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, trim

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    get_month_boundaries,
    gold_daily_trips_path,
    gold_hourly_trips_path,
    gold_location_pair_stats_path,
    gold_payment_type_stats_path,
    validate_config,
)


EXPECTED_COLUMNS: Dict[str, List[str]] = {
    "gold_daily_trips": [
        "pickup_date",
        "trips_count",
        "total_revenue",
        "avg_check",
        "avg_trip_distance",
        "avg_trip_duration_minutes",
        "short_trips_count",
        "medium_trips_count",
        "long_trips_count",
        "year",
        "month",
        "gold_load_timestamp",
    ],
    "gold_hourly_trips": [
        "pickup_date",
        "pickup_hour",
        "trips_count",
        "total_revenue",
        "avg_check",
        "avg_trip_distance",
        "avg_trip_duration_minutes",
        "year",
        "month",
        "gold_load_timestamp",
    ],
    "gold_location_pair_stats": [
        "pickup_date",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
        "trips_count",
        "total_revenue",
        "avg_check",
        "avg_trip_distance",
        "avg_trip_duration_minutes",
        "year",
        "month",
        "gold_load_timestamp",
    ],
    "gold_payment_type_stats": [
        "pickup_date",
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
    ],
}


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


def assert_expected_columns(df: DataFrame, table_name: str) -> None:
    expected_columns = EXPECTED_COLUMNS[table_name]
    actual_columns = set(df.columns)

    missing_columns = [
        column_name
        for column_name in expected_columns
        if column_name not in actual_columns
    ]

    if missing_columns:
        raise AssertionError(
            f"Gold table {table_name} is missing expected columns: "
            f"{missing_columns}. Actual columns: {df.columns}"
        )

    print(f"Expected columns are present for {table_name}")


def assert_not_empty(df: DataFrame, table_name: str) -> int:
    rows_count = df.count()

    if rows_count <= 0:
        raise AssertionError(f"Gold table {table_name} is empty")

    print(f"Rows count: {rows_count}")

    return rows_count


def assert_pickup_date_inside_month(
    df: DataFrame,
    table_name: str,
    month_start: str,
    next_month_start: str,
) -> None:
    outside_month_count = df.filter(
        col("pickup_date").isNull()
        | (col("pickup_date") < lit(month_start).cast("date"))
        | (col("pickup_date") >= lit(next_month_start).cast("date"))
    ).count()

    if outside_month_count > 0:
        raise AssertionError(
            f"Gold table {table_name} contains {outside_month_count} rows "
            f"outside expected month range "
            f"[{month_start}, {next_month_start})"
        )

    print(
        f"pickup_date range is valid for {table_name}: "
        f"[{month_start}, {next_month_start})"
    )


def assert_pickup_hour_is_valid(df: DataFrame, table_name: str) -> None:
    invalid_hour_count = df.filter(
        col("pickup_hour").isNull()
        | (col("pickup_hour") < 0)
        | (col("pickup_hour") > 23)
    ).count()

    if invalid_hour_count > 0:
        raise AssertionError(
            f"Gold table {table_name} contains {invalid_hour_count} rows "
            "with invalid pickup_hour"
        )

    print(f"pickup_hour values are valid for {table_name}")


def assert_string_columns_not_empty(
    df: DataFrame,
    table_name: str,
    column_names: List[str],
) -> None:
    for column_name in column_names:
        empty_count = df.filter(
            col(column_name).isNull()
            | (trim(col(column_name)) == "")
        ).count()

        if empty_count > 0:
            raise AssertionError(
                f"Gold table {table_name} contains {empty_count} rows "
                f"with empty {column_name}"
            )

        print(f"{column_name} values are not empty for {table_name}")


def check_gold_table(
    spark: SparkSession,
    table_name: str,
    path: str,
    month_start: str,
    next_month_start: str,
) -> None:
    print("=" * 80)
    print(f"Checking gold table: {table_name}")
    print(f"Path: {path}")

    df = spark.read.parquet(path)

    print("Schema:")
    df.printSchema()

    assert_expected_columns(df, table_name)
    assert_not_empty(df, table_name)
    assert_pickup_date_inside_month(
        df=df,
        table_name=table_name,
        month_start=month_start,
        next_month_start=next_month_start,
    )

    if table_name == "gold_hourly_trips":
        assert_pickup_hour_is_valid(df, table_name)

    if table_name == "gold_payment_type_stats":
        assert_string_columns_not_empty(
            df=df,
            table_name=table_name,
            column_names=["payment_type_name"],
        )

    if table_name == "gold_location_pair_stats":
        assert_string_columns_not_empty(
            df=df,
            table_name=table_name,
            column_names=["pickup_zone", "dropoff_zone"],
        )

    print("Sample rows:")
    df.show(5, truncate=False)

    print(f"Gold table quality check passed: {table_name}")


def main(year: str, month: str) -> None:
    month_start, next_month_start = get_month_boundaries(year, month)

    print("Starting gold Object Storage quality checks")
    print(f"Expected pickup_date range: [{month_start}, {next_month_start})")

    spark = create_spark_session()

    try:
        gold_tables = {
            "gold_daily_trips": gold_daily_trips_path(year, month),
            "gold_hourly_trips": gold_hourly_trips_path(year, month),
            "gold_location_pair_stats": gold_location_pair_stats_path(year, month),
            "gold_payment_type_stats": gold_payment_type_stats_path(year, month),
        }

        for table_name, path in gold_tables.items():
            check_gold_table(
                spark=spark,
                table_name=table_name,
                path=path,
                month_start=month_start,
                next_month_start=next_month_start,
            )

        print("Gold Object Storage quality checks completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)