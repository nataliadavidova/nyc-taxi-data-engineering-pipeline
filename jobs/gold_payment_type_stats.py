"""
Gold job: payment type statistics.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, lit, round, sum, when

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_payment_type_stats_path,
    silver_yellow_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_gold_payment_type_stats")
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
    gold_path = gold_payment_type_stats_path(year, month)

    print(f"Reading silver data from: {silver_path}")

    silver_df = spark.read.parquet(silver_path)

    print(f"Silver rows count: {silver_df.count()}")

    enriched_df = silver_df.withColumn(
        "payment_type_name",
        when(col("payment_type") == 1, lit("Credit card"))
        .when(col("payment_type") == 2, lit("Cash"))
        .when(col("payment_type") == 3, lit("No charge"))
        .when(col("payment_type") == 4, lit("Dispute"))
        .when(col("payment_type") == 5, lit("Unknown"))
        .when(col("payment_type") == 6, lit("Voided trip"))
        .otherwise(lit("Other")),
    )

    gold_df = (
        enriched_df
        .groupBy("pickup_date", "trip_type", "payment_type", "payment_type_name")
        .agg(
            count("*").alias("trips_count"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("total_amount"), 2).alias("avg_check"),
            round(sum("tip_amount"), 2).alias("total_tips"),
            round(avg("tip_amount"), 2).alias("avg_tip"),
            round(
                sum("tip_amount") / sum("total_amount"),
                4,
            ).alias("tips_share_from_revenue"),
        )
        .withColumn("year", col("pickup_date").substr(1, 4))
        .withColumn("month", col("pickup_date").substr(6, 2))
        .withColumn("gold_load_timestamp", current_timestamp())
        .orderBy("pickup_date", "trip_type", col("trips_count").desc())
    )

    print("Gold payment type preview:")
    gold_df.show(20, truncate=False)

    print(f"Writing gold data to: {gold_path}")

    (
        gold_df
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print("Gold payment type stats job completed successfully")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)