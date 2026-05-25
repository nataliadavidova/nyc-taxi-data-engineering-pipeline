"""
Silver job for NYC Yellow Taxi data.

What this job does:
1. Reads the Bronze layer from Yandex Object Storage.
2. Adds technical and analytical fields required for data quality checks.
3. Performs row-level data quality checks.
4. Writes bad records separately.
5. Builds the clean Silver dataset.
6. Validates the Silver dataset before writing it.
7. Writes the Silver layer.
8. Writes the quality report.

Optimization decisions:
1. Do not run a separate df.count() at the beginning of the job.
   Why:
   - df.count() is a separate Spark action;
   - it forces Spark to scan the monthly Bronze parquet dataset;
   - the following DQ aggregate would scan the same data again.
   Instead, total_count is calculated inside the shared DQ aggregate.

2. Calculate DQ metrics with one aggregate.
   Why:
   - separate count() actions for every DQ flag would trigger multiple Spark actions;
   - now all DQ flags, total_count, and total_bad are calculated in one pass.

3. Persist dq_df with StorageLevel.DISK_ONLY.
   Why:
   - dq_df is reused for DQ aggregation, bad records, clean records,
     Silver validation, and Silver writing;
   - DISK_ONLY reduces recomputation without putting additional pressure
     on the JVM heap.

4. Do not persist silver_df.
   Why:
   - silver_df is built from the already persisted dq_df;
   - persisting silver_df as well may increase disk or memory pressure;
   - this avoids unnecessary cache pressure in a local Docker/Spark environment.

5. Use try/finally.
   Why:
   - SparkSession should be stopped even if the job fails;
   - persisted dq_df should be released even if the job fails.
"""

import argparse
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    concat_ws,
    count as spark_count,
    current_timestamp,
    date_format,
    hour,
    lit,
    sum as spark_sum,
    to_date,
    unix_timestamp,
    when,
)
from pyspark.storagelevel import StorageLevel

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    bad_records_yellow_path,
    bronze_yellow_path,
    get_month_boundaries,
    quality_yellow_path,
    silver_yellow_path,
    validate_config,
)


VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    validate_config() runs before SparkSession creation so the job fails fast
    if required Object Storage configuration is missing.
    """

    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_silver_yellow")
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


def add_dq_columns(
    df: DataFrame,
    month_start: str,
    next_month_start: str,
) -> DataFrame:
    """
    Add trip duration and data quality columns to the Bronze dataframe.

    Why this function exists:
    - it contains pure transformation logic;
    - it does not read from S3;
    - it does not write parquet;
    - it can be tested with a small in-memory Spark DataFrame.

    This makes Silver transformation rules testable without running the full job.
    """

    dq_df = df.withColumn(
        "trip_duration_minutes",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        )
        / 60,
    )

    dq_df = (
        dq_df
        .withColumn("dq_null_pickup", col("tpep_pickup_datetime").isNull())
        .withColumn(
            "dq_outside_month",
            col("tpep_pickup_datetime").isNotNull()
            & (
                (to_date("tpep_pickup_datetime") < lit(month_start).cast("date"))
                | (
                    to_date("tpep_pickup_datetime")
                    >= lit(next_month_start).cast("date")
                )
            ),
        )
        .withColumn("dq_null_dropoff", col("tpep_dropoff_datetime").isNull())
        .withColumn(
            "dq_wrong_time",
            col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"),
        )
        .withColumn("dq_bad_distance", col("trip_distance") <= 0)
        .withColumn("dq_bad_fare", col("fare_amount") < 0)
        .withColumn("dq_bad_total", col("total_amount") < 0)
        .withColumn(
            "dq_bad_passenger",
            col("passenger_count").isNotNull() & (col("passenger_count") <= 0),
        )
        .withColumn(
            "dq_bad_payment_type",
            col("payment_type").isNull()
            | (~col("payment_type").isin(VALID_PAYMENT_TYPES)),
        )
        .withColumn(
            "dq_bad_pickup_location",
            col("PULocationID").isNull() | (col("PULocationID") <= 0),
        )
        .withColumn(
            "dq_bad_dropoff_location",
            col("DOLocationID").isNull() | (col("DOLocationID") <= 0),
        )
        .withColumn(
            "dq_bad_duration",
            (col("trip_duration_minutes") <= 0)
            | (col("trip_duration_minutes") > 1440),
        )
        .withColumn("dq_outlier_distance", col("trip_distance") > 100)
    )

    return dq_df


def build_silver_dataframe(
    clean_df: DataFrame,
    dq_cols: List[str],
) -> DataFrame:
    """
    Build the final Silver dataframe from clean records.

    Why this function exists:
    - it contains business transformation logic for the Silver layer;
    - it can be tested separately from S3 reads and writes;
    - unit tests can validate pickup_date, pickup_hour, pickup_month,
      trip_type, and removed DQ columns.
    """

    return (
        clean_df
        .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        .withColumn("pickup_month", date_format("tpep_pickup_datetime", "yyyy-MM"))
        .withColumn(
            "trip_type",
            when(col("trip_distance") < 2, "short")
            .when(col("trip_distance") <= 10, "medium")
            .otherwise("long"),
        )
        .withColumn("silver_load_timestamp", current_timestamp())
        .drop(*dq_cols)
    )


def build_bad_condition(dq_cols: List[str]) -> Column:
    """
    Build one boolean OR condition across all DQ flags.

    A row is treated as a bad record if at least one DQ flag is True.

    The previous implementation used a temporary None value and a for loop.
    The current implementation uses reduce(), which makes the OR aggregation
    across DQ flags explicit and avoids temporary mutable state.
    """

    if not dq_cols:
        raise ValueError("No data quality columns found")

    return reduce(
        lambda condition, c: condition | col(c),
        dq_cols[1:],
        col(dq_cols[0]),
    )


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    # dq_df is defined before the try block so it can be safely unpersisted
    # in finally. If the job fails before dq_df is created, it remains None.
    dq_df = None

    try:
        bronze_path = bronze_yellow_path(year, month)
        silver_path = silver_yellow_path(year, month)
        bad_records_path = bad_records_yellow_path(year, month)
        quality_path = quality_yellow_path(year, month)

        print(f"Reading bronze: {bronze_path}")
        df = spark.read.parquet(bronze_path)

        # Do not run df.count() here.
        #
        # A separate count() would trigger an additional Spark action and
        # force a full scan of the monthly Bronze parquet dataset in Object Storage.
        #
        # total_count is calculated later together with all DQ metrics in one
        # aggregate action.

        month_start, next_month_start = get_month_boundaries(year, month)

        print(f"Expected pickup date range: [{month_start}, {next_month_start})")

        # ======================
        # DATA QUALITY COLUMNS
        # ======================

        # Add trip_duration_minutes and all DQ flags.
        # trip_duration_minutes is required for dq_bad_duration.
        dq_df = add_dq_columns(
            df=df,
            month_start=month_start,
            next_month_start=next_month_start,
        )

        dq_cols = [c for c in dq_df.columns if c.startswith("dq_")]

        # dq_df is reused several times:
        # 1. DQ aggregate;
        # 2. bad records write;
        # 3. clean_df and silver_df creation;
        # 4. Silver quality checks;
        # 5. Silver write.
        #
        # Persisting dq_df reduces recomputation of the DQ transformation chain.
        #
        # Why DISK_ONLY:
        # - monthly taxi data can be large;
        # - MEMORY_ONLY or MEMORY_AND_DISK may increase JVM heap pressure;
        # - DISK_ONLY is safer for the local Docker/Spark environment.
        dq_df = dq_df.persist(StorageLevel.DISK_ONLY)

        # ======================
        # BAD RECORDS CONDITION
        # ======================

        # A row is bad if at least one DQ flag is True.
        bad_condition = build_bad_condition(dq_cols)

        # ======================
        # DATA QUALITY AGGREGATION
        # ======================

        # total_count is calculated here together with all DQ flag counts.
        #
        # This replaces a separate df.count() at the beginning of the job.
        #
        # Before:
        # - action 1: df.count()
        # - action 2: dq_df.agg(...).collect()
        #
        # After:
        # - action 1: dq_df.agg(total_count, dq flags, total_bad).collect()
        #
        # This removes one redundant full scan of the Bronze dataset.
        report_agg_expressions = [
            spark_count("*").cast("long").alias("total_count"),
            *[
                spark_sum(when(col(c), 1).otherwise(0)).cast("long").alias(c)
                for c in dq_cols
            ],
        ]

        report_agg_expressions.append(
            spark_sum(when(bad_condition, 1).otherwise(0))
            .cast("long")
            .alias("total_bad")
        )

        report_counts = dq_df.agg(*report_agg_expressions).collect()[0]

        total_count = int(report_counts["total_count"])
        bad_count = int(report_counts["total_bad"])

        print(f"Total rows: {total_count}")
        print(f"Bad rows: {bad_count}")

        # ======================
        # BAD RECORDS
        # ======================

        bad_df = dq_df.filter(bad_condition)

        # dq_reason stores the list of DQ flags that failed for each bad record.
        # This makes bad records easier to inspect and debug.
        bad_df = bad_df.withColumn(
            "dq_reason",
            concat_ws(
                "; ",
                *[when(col(c), lit(c)) for c in dq_cols],
            ),
        )

        print(f"Writing bad records to: {bad_records_path}")
        bad_df.write.mode("overwrite").parquet(bad_records_path)

        # ======================
        # CLEAN DATA
        # ======================

        # clean_df contains only records where no DQ flag is True.
        clean_df = dq_df.filter(~bad_condition)

        # silver_df is the cleaned analytical layer.
        #
        # It adds:
        # - pickup_date for daily and monthly analytics;
        # - pickup_hour for hourly demand analytics;
        # - pickup_month for monthly trends;
        # - trip_type for short, medium, and long trip analysis;
        # - silver_load_timestamp for auditability.
        #
        # DQ columns are dropped because they are quality-control metadata and
        # should not be part of the clean Silver contract.
        silver_df = build_silver_dataframe(
            clean_df=clean_df,
            dq_cols=dq_cols,
        )

        # ======================
        # SILVER QUALITY CHECKS
        # ======================

        # Validate the clean Silver dataset before writing:
        # - pickup_date must be inside the expected month;
        # - pickup_hour must be in the 0..23 range;
        # - silver_count is calculated for logging and row-loss reporting.
        #
        # This is a separate action, but it is based on the persisted dq_df,
        # so it should not re-read the Bronze parquet dataset from Object Storage.
        silver_quality_counts = silver_df.agg(
            spark_count("*").cast("long").alias("silver_count"),
            spark_sum(
                when(
                    (col("pickup_date") < lit(month_start).cast("date"))
                    | (col("pickup_date") >= lit(next_month_start).cast("date")),
                    1,
                ).otherwise(0)
            ).cast("long").alias("outside_month_count"),
            spark_sum(
                when(
                    col("pickup_hour").isNull()
                    | (col("pickup_hour") < 0)
                    | (col("pickup_hour") > 23),
                    1,
                ).otherwise(0)
            ).cast("long").alias("invalid_pickup_hour_count"),
        ).collect()[0]

        silver_count = int(silver_quality_counts["silver_count"])
        outside_month_count = int(silver_quality_counts["outside_month_count"])
        invalid_pickup_hour_count = int(
            silver_quality_counts["invalid_pickup_hour_count"]
        )

        if outside_month_count > 0:
            raise ValueError(
                f"Silver contains {outside_month_count} rows outside "
                f"expected pickup date range [{month_start}, {next_month_start})"
            )

        if invalid_pickup_hour_count > 0:
            raise ValueError(
                f"Silver contains {invalid_pickup_hour_count} rows "
                "with invalid pickup_hour"
            )

        print(f"Clean rows: {silver_count}")
        print(f"Removed rows: {total_count - silver_count}")

        print(f"Writing silver data to: {silver_path}")
        silver_df.write.mode("overwrite").parquet(silver_path)

        # ======================
        # QUALITY REPORT
        # ======================

        # Build a small quality report from the already calculated report_counts.
        #
        # Important:
        # this does not run additional count() actions for each DQ flag.
        # All values come from the shared aggregate above.
        report_data = []

        for c in dq_cols:
            failed = int(report_counts[c])
            share = failed / total_count if total_count else 0.0
            report_data.append((c, failed, total_count, share))

        total_bad_share = bad_count / total_count if total_count else 0.0
        report_data.append(("total_bad", bad_count, total_count, total_bad_share))

        report_df = (
            spark.createDataFrame(
                report_data,
                ["check", "failed_rows", "total_rows", "share"],
            )
            .withColumn("created_at", current_timestamp())
        )

        # report_df is small: one row per DQ check.
        # show() is useful in Airflow logs and does not add meaningful load here.
        report_df.show(truncate=False)

        print(f"Writing quality report to: {quality_path}")
        report_df.write.mode("overwrite").parquet(quality_path)

        print("Silver job completed successfully")

    finally:
        # If dq_df was persisted, release it even if the job fails.
        # This avoids leaving unnecessary persisted blocks in Spark.
        if dq_df is not None:
            dq_df.unpersist()

        # Always stop SparkSession.
        # If the job fails during read, write, or validation, Spark still stops.
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)