"""
Gold job: daily trips analytics.
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

    silver_path = silver_yellow_path(year, month)
    gold_path = gold_daily_trips_path(year, month)

    print(f"Reading silver data from: {silver_path}")

    silver_df = spark.read.parquet(silver_path)

    print(f"Silver rows count: {silver_df.count()}")

    gold_df = (
        silver_df
        .groupBy("pickup_date")
        .agg(
            count("*").alias("trips_count"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("total_amount"), 2).alias("avg_check"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
            sum(when(col("trip_type") == "short", 1).otherwise(0)).alias("short_trips_count"),
            sum(when(col("trip_type") == "medium", 1).otherwise(0)).alias("medium_trips_count"),
            sum(when(col("trip_type") == "long", 1).otherwise(0)).alias("long_trips_count"),
        )
        .withColumn("year", col("pickup_date").substr(1, 4))
        .withColumn("month", col("pickup_date").substr(6, 2))
        .withColumn("gold_load_timestamp", current_timestamp())
        .orderBy("pickup_date")
    )

    print("Gold daily preview:")
    gold_df.show(31, truncate=False)

    print(f"Writing gold data to: {gold_path}")

    (
        gold_df
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print("Gold daily trips job completed successfully")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)