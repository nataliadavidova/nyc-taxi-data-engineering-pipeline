"""
Silver job for NYC Yellow Taxi data (with .env support).

Что делает:
1. Читает bronze-слой
2. Делает data quality checks
3. Сохраняет bad_records
4. Чистит данные
5. Пишет silver
6. Пишет quality report
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    date_format,
    hour,
    lit,
    to_date,
    unix_timestamp,
    when,
)

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    bad_records_yellow_path,
    bronze_yellow_path,
    quality_yellow_path,
    silver_yellow_path,
    validate_config,
    get_month_boundaries,
)

VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]

def create_spark_session() -> SparkSession:
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

def main(year: str, month: str) -> None:
    spark = create_spark_session()

    bronze_path = bronze_yellow_path(year, month)
    silver_path = silver_yellow_path(year, month)
    bad_records_path = bad_records_yellow_path(year, month)
    quality_path = quality_yellow_path(year, month)

    print(f"Reading bronze: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    total_count = df.count()
    print(f"Total rows: {total_count}")

    month_start, next_month_start = get_month_boundaries(year, month)

    print(f"Expected pickup date range: [{month_start}, {next_month_start})")

    # ======================
    # DATA QUALITY CHECKS
    # ======================

    dq_df = df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60,
    )

    dq_df = dq_df \
        .withColumn("dq_null_pickup", col("tpep_pickup_datetime").isNull()) \
        .withColumn(
            "dq_outside_month",
            col("tpep_pickup_datetime").isNotNull()
            & (
                (to_date("tpep_pickup_datetime") < lit(month_start).cast("date"))
                | (to_date("tpep_pickup_datetime") >= lit(next_month_start).cast("date"))
            ),
        ) \
        .withColumn("dq_null_dropoff", col("tpep_dropoff_datetime").isNull()) \
        .withColumn("dq_wrong_time", col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime")) \
        .withColumn("dq_bad_distance", col("trip_distance") <= 0) \
        .withColumn("dq_bad_fare", col("fare_amount") < 0) \
        .withColumn("dq_bad_total", col("total_amount") < 0) \
        .withColumn("dq_bad_passenger", col("passenger_count").isNotNull() & (col("passenger_count") <= 0)) \
        .withColumn(
            "dq_bad_payment_type",
            col("payment_type").isNull()
            | (~col("payment_type").isin(VALID_PAYMENT_TYPES)),
        ) \
        .withColumn(
            "dq_bad_pickup_location",
            col("PULocationID").isNull() | (col("PULocationID") <= 0),
        ) \
        .withColumn(
            "dq_bad_dropoff_location",
            col("DOLocationID").isNull() | (col("DOLocationID") <= 0),
        ) \
        .withColumn("dq_bad_duration", (col("trip_duration_minutes") <= 0) | (col("trip_duration_minutes") > 1440)) \
        .withColumn("dq_outlier_distance", col("trip_distance") > 100)


    dq_cols = [c for c in dq_df.columns if c.startswith("dq_")]

    # ======================
    # BAD RECORDS
    # ======================

    bad_condition = None
    for c in dq_cols:
        bad_condition = col(c) if bad_condition is None else (bad_condition | col(c))

    bad_df = dq_df.filter(bad_condition)

    bad_df = bad_df.withColumn(
        "dq_reason",
        concat_ws("; ",
            *[when(col(c), lit(c)) for c in dq_cols]
        )
    )

    bad_count = bad_df.count()
    print(f"Bad rows: {bad_count}")

    bad_df.write.mode("overwrite").parquet(bad_records_path)

    # ======================
    # CLEAN DATA
    # ======================

    clean_df = dq_df.filter(~bad_condition)

    silver_df = clean_df \
        .withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
        .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("pickup_month", date_format("tpep_pickup_datetime", "yyyy-MM")) \
        .withColumn(
            "trip_type",
            when(col("trip_distance") < 2, "short")
            .when(col("trip_distance") <= 10, "medium")
            .otherwise("long")
        ) \
        .withColumn("silver_load_timestamp", current_timestamp()) \
        .drop(*dq_cols)

    silver_count = silver_df.count()

    outside_month_count = silver_df.filter(
        (col("pickup_date") < lit(month_start).cast("date"))
        | (col("pickup_date") >= lit(next_month_start).cast("date"))
    ).count()

    if outside_month_count > 0:
        raise ValueError(
            f"Silver contains {outside_month_count} rows outside "
            f"expected pickup date range [{month_start}, {next_month_start})"
        )

    invalid_pickup_hour_count = silver_df.filter(
        col("pickup_hour").isNull()
        | (col("pickup_hour") < 0)
        | (col("pickup_hour") > 23)
    ).count()

    if invalid_pickup_hour_count > 0:
        raise ValueError(
            f"Silver contains {invalid_pickup_hour_count} rows "
            "with invalid pickup_hour"
        )

    print(f"Clean rows: {silver_count}")
    print(f"Removed rows: {total_count - silver_count}")

    silver_df.write.mode("overwrite").parquet(silver_path)

    # ======================
    # QUALITY REPORT
    # ======================

    report_data = []

    for c in dq_cols:
        failed = dq_df.filter(col(c)).count()
        report_data.append((c, failed, total_count, failed / total_count))

    report_data.append(("total_bad", bad_count, total_count, bad_count / total_count))

    report_df = spark.createDataFrame(
        report_data,
        ["check", "failed_rows", "total_rows", "share"]
    ).withColumn("created_at", current_timestamp())

    report_df.show(truncate=False)

    report_df.write.mode("overwrite").parquet(quality_path)

    print("Silver job DONE ✅")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(args.year, args.month)