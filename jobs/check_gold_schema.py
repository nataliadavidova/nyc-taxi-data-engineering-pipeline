"""
Check schemas and data quality for NYC Taxi gold marts in Object Storage.

What this job does:
1. Reads gold parquet marts from S3-compatible Object Storage.
2. Validates expected columns for each gold mart.
3. Validates that each gold mart is not empty.
4. Validates that pickup_date values belong to the expected processing month.
5. Validates mart-specific fields:
   - trip_type is not empty for hourly, route, and payment marts;
   - pickup_hour is between 0 and 23 for hourly trips;
   - payment_type_name is not empty for payment type stats;
   - pickup_zone and dropoff_zone are not empty for location pair stats.

Optimization decisions:
1. All data quality checks for each gold mart are calculated in one aggregate.
   Why:
   - previously each check used a separate .count();
   - each .count() is a separate Spark action;
   - Spark could repeatedly scan the same gold parquet files from Object Storage;
   - now row count, date checks, and mart-specific checks are calculated in one pass.

2. Schema validation is still done separately on the driver.
   Why:
   - checking df.columns does not trigger a Spark data scan;
   - it is cheap and should happen before aggregate expressions are built.

3. Removed df.show() from the production-like check.
   Why:
   - show() is also a Spark action;
   - it triggers an extra read/compute step only for logging;
   - for production checks, logs should focus on quality metrics;
   - sample rows can be inspected manually after the job if needed.

4. No cache/persist is used.
   Why:
   - after optimization, each gold mart is read for one main aggregate action;
   - caching would add unnecessary memory/disk pressure.

5. SparkSession is closed in try/finally.
   Why:
   - Spark should stop even if a schema or quality check fails.
"""

import argparse
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count as spark_count,
    lit,
    sum as spark_sum,
    trim,
    when,
)

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
        "trip_type",
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
        "trip_type",
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
    ],
}


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    validate_config() checks required Object Storage settings before Spark starts.
    """

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
    """
    Validate expected schema columns.

    This check uses df.columns, so it does not trigger a full Spark data scan.
    We keep it separate and run it before data quality aggregate checks.
    """

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


def empty_string_condition(column_name: str):
    """
    Build a reusable condition for empty string-like columns.

    A string value is considered invalid if it is:
    - NULL;
    - empty after trim().
    """

    return col(column_name).isNull() | (trim(col(column_name)) == "")


def build_quality_expressions(
    table_name: str,
    month_start: str,
    next_month_start: str,
):
    """
    Build all quality check expressions for a gold mart.

    Main optimization:
    instead of running many df.filter(...).count() actions, we build all checks
    as aggregate expressions and run one df.agg(...).collect() per table.
    """

    expressions = [
        spark_count("*").cast("long").alias("rows_count"),
        spark_sum(
            when(
                col("pickup_date").isNull()
                | (col("pickup_date") < lit(month_start).cast("date"))
                | (col("pickup_date") >= lit(next_month_start).cast("date")),
                1,
            ).otherwise(0)
        ).cast("long").alias("outside_month_count"),
    ]

    if table_name in [
        "gold_hourly_trips",
        "gold_location_pair_stats",
        "gold_payment_type_stats",
    ]:
        expressions.append(
            spark_sum(
                when(empty_string_condition("trip_type"), 1).otherwise(0)
            ).cast("long").alias("empty_trip_type_count")
        )

    if table_name == "gold_hourly_trips":
        expressions.append(
            spark_sum(
                when(
                    col("pickup_hour").isNull()
                    | (col("pickup_hour") < 0)
                    | (col("pickup_hour") > 23),
                    1,
                ).otherwise(0)
            ).cast("long").alias("invalid_pickup_hour_count")
        )

    if table_name == "gold_payment_type_stats":
        expressions.append(
            spark_sum(
                when(empty_string_condition("payment_type_name"), 1).otherwise(0)
            ).cast("long").alias("empty_payment_type_name_count")
        )

    if table_name == "gold_location_pair_stats":
        expressions.extend(
            [
                spark_sum(
                    when(empty_string_condition("pickup_zone"), 1).otherwise(0)
                ).cast("long").alias("empty_pickup_zone_count"),
                spark_sum(
                    when(empty_string_condition("dropoff_zone"), 1).otherwise(0)
                ).cast("long").alias("empty_dropoff_zone_count"),
            ]
        )

    return expressions


def validate_quality_counts(
    table_name: str,
    quality_counts,
    month_start: str,
    next_month_start: str,
) -> None:
    """
    Validate aggregate quality check results.

    quality_counts is a single Row collected from Spark.
    All checks below are driver-side checks and do not trigger new Spark actions.
    """

    rows_count = int(quality_counts["rows_count"])
    outside_month_count = int(quality_counts["outside_month_count"])

    print(f"Rows count: {rows_count}")

    if rows_count <= 0:
        raise AssertionError(f"Gold table {table_name} is empty")

    print(f"Rows outside expected month: {outside_month_count}")

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

    if table_name in [
        "gold_hourly_trips",
        "gold_location_pair_stats",
        "gold_payment_type_stats",
    ]:
        empty_trip_type_count = int(quality_counts["empty_trip_type_count"])
        print(f"Empty trip_type count: {empty_trip_type_count}")

        if empty_trip_type_count > 0:
            raise AssertionError(
                f"Gold table {table_name} contains "
                f"{empty_trip_type_count} rows with empty trip_type"
            )

    if table_name == "gold_hourly_trips":
        invalid_pickup_hour_count = int(
            quality_counts["invalid_pickup_hour_count"]
        )
        print(f"Invalid pickup_hour count: {invalid_pickup_hour_count}")

        if invalid_pickup_hour_count > 0:
            raise AssertionError(
                f"Gold table {table_name} contains "
                f"{invalid_pickup_hour_count} rows with invalid pickup_hour"
            )

    if table_name == "gold_payment_type_stats":
        empty_payment_type_name_count = int(
            quality_counts["empty_payment_type_name_count"]
        )
        print(f"Empty payment_type_name count: {empty_payment_type_name_count}")

        if empty_payment_type_name_count > 0:
            raise AssertionError(
                f"Gold table {table_name} contains "
                f"{empty_payment_type_name_count} rows with empty payment_type_name"
            )

    if table_name == "gold_location_pair_stats":
        empty_pickup_zone_count = int(quality_counts["empty_pickup_zone_count"])
        empty_dropoff_zone_count = int(quality_counts["empty_dropoff_zone_count"])

        print(f"Empty pickup_zone count: {empty_pickup_zone_count}")
        print(f"Empty dropoff_zone count: {empty_dropoff_zone_count}")

        if empty_pickup_zone_count > 0:
            raise AssertionError(
                f"Gold table {table_name} contains "
                f"{empty_pickup_zone_count} rows with empty pickup_zone"
            )

        if empty_dropoff_zone_count > 0:
            raise AssertionError(
                f"Gold table {table_name} contains "
                f"{empty_dropoff_zone_count} rows with empty dropoff_zone"
            )


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

    # Main optimization:
    # run one aggregate action per table instead of many separate count actions.
    quality_expressions = build_quality_expressions(
        table_name=table_name,
        month_start=month_start,
        next_month_start=next_month_start,
    )

    quality_counts = df.agg(*quality_expressions).collect()[0]

    validate_quality_counts(
        table_name=table_name,
        quality_counts=quality_counts,
        month_start=month_start,
        next_month_start=next_month_start,
    )

    # We intentionally do not call df.show() here.
    #
    # Why:
    # - show() is a Spark action;
    # - this job is a validation gate, not an exploratory notebook;
    # - if sample rows are needed, inspect the parquet output separately.
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