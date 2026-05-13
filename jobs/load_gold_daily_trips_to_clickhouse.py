"""
Loads Gold daily trips mart from S3/Object Storage to ClickHouse.

Source:
s3a://<bucket>/nyc_taxi/gold/yellow/daily_trips/year=<year>/month=<month>

Target:
nyc_taxi.gold_daily_trips
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BUCKET_NAME,
    S3_ENDPOINT,
    S3_REGION,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    validate_config,
)


def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_load_gold_daily_trips_to_clickhouse")
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

    gold_path = (
        f"s3a://{BUCKET_NAME}/nyc_taxi/gold/yellow/daily_trips/"
        f"year={year_arg}/month={month_arg}"
    )

    print(f"Reading Gold daily trips from: {gold_path}")

    df = spark.read.parquet(gold_path)

    df = df.select(
        col("pickup_date"),
        col("trips_count"),
        col("total_revenue"),
        col("avg_check"),
        col("avg_trip_distance"),
        col("avg_trip_duration_minutes"),
        col("short_trips_count"),
        col("medium_trips_count"),
        col("long_trips_count"),
        col("year"),
        col("month"),
        col("gold_load_timestamp"),
    )

    rows_count = df.count()
    print(f"Rows to load into ClickHouse: {rows_count}")

    if rows_count == 0:
        raise ValueError("No rows to load into ClickHouse")

    jdbc_url = (
        f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        f"{CLICKHOUSE_DATABASE}"
    )

    target_table = f"{CLICKHOUSE_DATABASE}.gold_daily_trips"

    print(f"Writing to ClickHouse: {target_table}")

    write_options = {
        "url": jdbc_url,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "dbtable": target_table,
        "user": CLICKHOUSE_USER,
    }

    if CLICKHOUSE_PASSWORD:
        write_options["password"] = CLICKHOUSE_PASSWORD

    writer = df.write.format("jdbc").mode("append")

    for option_name, option_value in write_options.items():
        writer = writer.option(option_name, option_value)

    writer.save()

    print("Gold hourly trips loaded to ClickHouse successfully")

    read_options = {
        "url": jdbc_url,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "dbtable": target_table,
        "user": CLICKHOUSE_USER,
    }

    if CLICKHOUSE_PASSWORD:
        read_options["password"] = CLICKHOUSE_PASSWORD

    reader = spark.read.format("jdbc")

    for option_name, option_value in read_options.items():
        reader = reader.option(option_name, option_value)

    loaded_df = reader.load()

    print(f"Rows in ClickHouse after load: {loaded_df.count()}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year_arg=args.year, month_arg=args.month)