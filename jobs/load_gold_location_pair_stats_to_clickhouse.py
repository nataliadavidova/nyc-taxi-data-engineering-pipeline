"""
Load monthly gold location pair stats mart into ClickHouse.

What this job does:
1. Reads the monthly gold_location_pair_stats parquet mart from Object Storage.
2. Selects columns in the exact order expected by the ClickHouse table.
3. Validates that the monthly gold parquet is not empty.
4. Appends monthly rows to ClickHouse.
5. Checks how many rows exist in ClickHouse for the loaded year/month.

Optimization decisions:
1. Replaced df.count() before load with df.take(1).
   Why:
   - gold_location_pair_stats is the largest gold mart;
   - df.count() scans the full monthly parquet dataset;
   - writer.save() then scans the same parquet again to load it into ClickHouse;
   - for a non-empty check, reading one row is enough.

2. Added targeted ClickHouse count for the loaded year/month.
   Why:
   - this gives useful observability after load;
   - it avoids scanning unrelated months;
   - full serving-layer validation is handled by check_clickhouse_gold_quality.py.

3. No full ClickHouse table count is used.
   Why:
   - full table counts become expensive as gold_location_pair_stats grows;
   - this load job is responsible only for the current monthly load.

4. Added try/finally.
   Why:
   - SparkSession should stop even if parquet read or JDBC write fails.

5. Kept mode("append").
   Why:
   - current full-year pipeline runs truncate_clickhouse_gold_tables.py before loading;
   - therefore monthly append is safe within the full refresh DAG;
   - future incremental loading should replace/reload only affected months.
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
    gold_location_pair_stats_path,
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
        .appName("nyc_taxi_load_gold_location_pair_stats_to_clickhouse")
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

    Keeping this in one helper reduces duplication and ensures that read/write
    operations use the same connection settings.
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

    Why:
    - this mart can be large;
    - counting the whole ClickHouse table after every monthly load is unnecessary;
    - the load job only needs a targeted sanity check;
    - full ClickHouse serving-layer validation is handled later by
      check_clickhouse_gold_quality.py.
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
        gold_path = gold_location_pair_stats_path(year_arg, month_arg)

        print(f"Reading Gold location pair stats from: {gold_path}")

        df = spark.read.parquet(gold_path)

        df = df.select(
            col("pickup_date"),
            col("trip_type"),
            col("pickup_location_id"),
            col("pickup_borough"),
            col("pickup_zone"),
            col("pickup_service_zone"),
            col("dropoff_location_id"),
            col("dropoff_borough"),
            col("dropoff_zone"),
            col("dropoff_service_zone"),
            col("trips_count"),
            col("total_revenue"),
            col("avg_check"),
            col("avg_trip_distance"),
            col("avg_trip_duration_minutes"),
            col("year"),
            col("month"),
            col("gold_load_timestamp"),
        )

        # ВАЖНО:
        # Не делаем df.count() перед загрузкой.
        #
        # Почему:
        # - gold_location_pair_stats — самый большой gold mart;
        # - count() полностью сканирует monthly parquet;
        # - writer.save() ниже снова сканирует тот же parquet для записи в ClickHouse;
        # - для проверки, что parquet не пустой, достаточно взять одну строку.
        if not df.take(1):
            raise ValueError("No rows to load into ClickHouse")

        target_table = f"{CLICKHOUSE_DATABASE}.gold_location_pair_stats"

        print(f"Writing to ClickHouse: {target_table}")

        writer = df.write.format("jdbc").mode("append")

        write_options = build_jdbc_options(target_table)

        for option_name, option_value in write_options.items():
            writer = writer.option(option_name, option_value)

        writer.save()

        print("Gold location pair stats loaded to ClickHouse successfully")

        # ВАЖНО:
        # Проверяем только текущий year/month, а не всю ClickHouse-таблицу.
        #
        # Почему:
        # - таблица location_pair_stats самая большая;
        # - full table count после каждого monthly load был бы лишним;
        # - финальная full-year проверка ClickHouse слоя выполняется отдельным job.
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