"""
Gold job: pickup/dropoff location pair statistics.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, current_timestamp, round, sum

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_location_pair_stats_path,
    silver_yellow_path,
    taxi_zone_lookup_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_gold_location_pair_stats")
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
    gold_path = gold_location_pair_stats_path(year, month)

    print(f"Reading silver data from: {silver_path}")

    silver_df = spark.read.parquet(silver_path)

    print(f"Silver rows count: {silver_df.count()}")

    lookup_path = taxi_zone_lookup_path()

    print(f"Reading taxi zone lookup from: {lookup_path}")

    zones_df = (
        spark.read
        .option("header", "true")
        .csv(lookup_path)
    )

    aggregated_df = (
        silver_df
        .groupBy("pickup_date", "trip_type", "PULocationID", "DOLocationID")
        .agg(
            count("*").alias("trips_count"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("total_amount"), 2).alias("avg_check"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        )
    )

    pickup_zones_df = (
        zones_df
        .select(
            col("LocationID").cast("int").alias("pickup_location_id"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone"),
            col("service_zone").alias("pickup_service_zone"),
        )
    )

    dropoff_zones_df = (
        zones_df
        .select(
            col("LocationID").cast("int").alias("dropoff_location_id"),
            col("Borough").alias("dropoff_borough"),
            col("Zone").alias("dropoff_zone"),
            col("service_zone").alias("dropoff_service_zone"),
        )
    )

    gold_df = (
        aggregated_df
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .join(
            broadcast(pickup_zones_df),
            on="pickup_location_id",
            how="left",
        )
        .join(
            broadcast(dropoff_zones_df),
            on="dropoff_location_id",
            how="left",
        )
        .withColumn("year", col("pickup_date").substr(1, 4))
        .withColumn("month", col("pickup_date").substr(6, 2))
        .withColumn("gold_load_timestamp", current_timestamp())
        .orderBy("pickup_date", "trip_type", col("trips_count").desc())
    )

    print("Gold location pair preview:")
    gold_df.show(20, truncate=False)

    print(f"Writing gold data to: {gold_path}")

    (
        gold_df
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print("Gold location pair stats job completed successfully")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)