"""
Load monthly gold daily trips mart into ClickHouse.

What this job does:
1. Reads the monthly gold_daily_trips parquet mart from Object Storage.
2. Selects columns in the exact order expected by the ClickHouse table.
3. Validates that the monthly gold parquet is not empty.
4. Appends monthly rows to ClickHouse.
5. Checks how many rows exist in ClickHouse for the loaded year/month.

Optimization decisions:
1. Replaced df.count() before load with df.take(1).
   Why:
   - df.count() scans the full parquet dataset;
   - write.save() then scans the same parquet again to load it into ClickHouse;
   - for a non-empty check, reading one row is enough.

2. Removed full ClickHouse table count after load.
   Why:
   - loaded_df.count() scans the whole target ClickHouse table;
   - this becomes expensive for larger marts;
   - this job only needs a lightweight sanity check for the current month.

3. Added targeted ClickHouse count for the loaded year/month.
   Why:
   - it gives useful observability without scanning unrelated months;
   - full serving-layer validation is handled by check_clickhouse_gold_quality.py.

4. Added try/finally.
   Why:
   - SparkSession should stop even if parquet read or JDBC write fails.

5. Kept mode("append").
   Why:
   - current full-year pipeline runs truncate_clickhouse_gold_tables.py before loading;
   - therefore monthly append is safe within the full refresh DAG.
   - future incremental loading should replace/reload only affected partitions/months.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    S3_ENDPOINT,
    S3_REGION,
    gold_daily_trips_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    validate_config() checks Object Storage and ClickHouse settings before
    the Spark job starts.
    """

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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def build_jdbc_url() -> str:
    return (
        f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        f"{CLICKHOUSE_DATABASE}"
    )


def build_jdbc_options(dbtable: str) -> dict:
    """
    Build common JDBC options for ClickHouse read/write operations.

    We keep this helper small so all JDBC reads/writes use the same connection
    settings and credentials.
    """

    options = {
        "url": build_jdbc_url(),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "dbtable": dbtable,
        "user": CLICKHOUSE_USER,
    }

    if CLICKHOUSE_PASSWORD:
        options["password"] = CLICKHOUSE_PASSWORD

    return options


def read_loaded_month_count(
    spark: SparkSession,
    target_table: str,
    year_arg: str,
    month_arg: str,
) -> int:
    """
    Read row count from ClickHouse only for the loaded year/month.

    This replaces the old loaded_df.count() over the full target table.

    Why:
    - full table count becomes expensive when the table grows;
    - current load job only needs a targeted sanity check;
    - final full-table validation is handled by check_clickhouse_gold_quality.py.
    """

    count_query = (
        f"(SELECT count(*) AS rows_count "
        f"FROM {target_table} "
        f"WHERE year = '{year_arg}' AND month = '{month_arg}') AS loaded_month_count"
    )

    reader = spark.read.format("jdbc")

    for option_name, option_value in build_jdbc_options(count_query).items():
        reader = reader.option(option_name, option_value)

    result_row = reader.load().collect()[0]

    return int(result_row["rows_count"])


def main(year_arg: str, month_arg: str) -> None:
    spark = create_spark_session()

    try:
        gold_path = gold_daily_trips_path(year_arg, month_arg)

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

        # ВАЖНО:
        # Не делаем df.count() перед загрузкой.
        #
        # Почему:
        # - count() полностью сканирует parquet;
        # - writer.save() ниже снова сканирует parquet для записи в ClickHouse;
        # - для проверки, что parquet не пустой, достаточно взять одну строку.
        if not df.take(1):
            raise ValueError("No rows to load into ClickHouse")

        target_table = f"{CLICKHOUSE_DATABASE}.gold_daily_trips"

        print(f"Writing to ClickHouse: {target_table}")

        writer = df.write.format("jdbc").mode("append")

        write_options = build_jdbc_options(target_table)

        for option_name, option_value in write_options.items():
            writer = writer.option(option_name, option_value)

        writer.save()

        print("Gold daily trips loaded to ClickHouse successfully")

        loaded_month_count = read_loaded_month_count(
            spark=spark,
            target_table=target_table,
            year_arg=year_arg,
            month_arg=month_arg,
        )

        print(
            f"Rows in ClickHouse for year={year_arg}, month={month_arg}: "
            f"{loaded_month_count}"
        )

        if loaded_month_count <= 0:
            raise ValueError(
                f"No rows found in ClickHouse after load for "
                f"year={year_arg}, month={month_arg}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year_arg=args.year, month_arg=args.month)