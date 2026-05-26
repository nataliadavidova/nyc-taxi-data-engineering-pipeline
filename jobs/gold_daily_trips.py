"""
Gold job: daily trips analytics.

What this job does:
1. Reads the monthly Silver layer from Object Storage.
2. Builds a daily analytical mart.
3. Writes the daily Gold mart back to Object Storage.

Optimization decisions:
1. Do not run silver_df.count() only for logging.
   Why:
   - count() is a separate Spark action;
   - the write action already needs to scan the dataset.

2. Do not run gold_df.show().
   Why:
   - show() is also a Spark action;
   - production-like jobs should avoid preview actions unless they are required.

3. Do not order data before writing parquet.
   Why:
   - row order is not a reliable storage contract for parquet datasets;
   - ordering should be done in SQL or BI queries when reading data.

4. Put transformation logic into build_gold_daily_trips().
   Why:
   - the function contains pure Spark transformation logic;
   - it can be tested with a small in-memory Spark DataFrame;
   - tests do not need S3 or parquet writes.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    round,
    sum,
    when,
)

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
    """
    Create SparkSession with S3A settings.

    validate_config() runs before SparkSession creation so the job fails fast
    if required Object Storage configuration is missing.
    """

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


def build_gold_daily_trips(silver_df: DataFrame) -> DataFrame:
    """
    Build the daily trips Gold mart from the Silver dataframe.

    The output is aggregated by pickup_date and contains:
    - trip count;
    - total revenue;
    - average check;
    - average distance;
    - average duration;
    - trip counts by trip type;
    - year/month partition helper columns;
    - gold load timestamp.

    This function does not read or write data. It contains only transformation
    logic and can be tested with a small in-memory Spark DataFrame.
    """

    return (
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
            sum(when(col("trip_type") == "short", 1).otherwise(0)).alias(
                "short_trips_count"
            ),
            sum(when(col("trip_type") == "medium", 1).otherwise(0)).alias(
                "medium_trips_count"
            ),
            sum(when(col("trip_type") == "long", 1).otherwise(0)).alias(
                "long_trips_count"
            ),
        )
        .withColumn("year", date_format(col("pickup_date"), "yyyy"))
        .withColumn("month", date_format(col("pickup_date"), "MM"))
        .withColumn("gold_load_timestamp", current_timestamp())
    )


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    try:
        silver_path = silver_yellow_path(year, month)
        gold_path = gold_daily_trips_path(year, month)

        print(f"Reading silver data from: {silver_path}")

        silver_df = spark.read.parquet(silver_path)

        # Do not run silver_df.count() here.
        #
        # count() would trigger an extra Spark action and a full scan of the
        # Silver parquet dataset. The write action below already scans the data.
        gold_df = build_gold_daily_trips(silver_df)

        print(f"Writing gold daily trips data to: {gold_path}")

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