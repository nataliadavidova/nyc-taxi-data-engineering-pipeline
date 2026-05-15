"""
Create ClickHouse gold tables for NYC Taxi pipeline.

This script is idempotent:
- creates the ClickHouse database if it does not exist;
- creates all gold tables if they do not exist;
- does not drop or overwrite existing tables.

It should run before loading gold marts into ClickHouse.
"""

from truncate_clickhouse_gold_tables import (
    CLICKHOUSE_DATABASE,
    execute_clickhouse_query,
)


def create_database() -> None:
    query = f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}"

    print("Creating ClickHouse database if not exists:")
    print(query)

    execute_clickhouse_query(query)


def create_gold_daily_trips_table() -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_daily_trips
    (
        pickup_date Date,
        trips_count UInt32,
        total_revenue Float64,
        avg_check Float64,
        avg_trip_distance Float64,
        avg_trip_duration_minutes Float64,
        short_trips_count UInt32,
        medium_trips_count UInt32,
        long_trips_count UInt32,
        year String,
        month String,
        gold_load_timestamp DateTime
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(pickup_date)
    ORDER BY pickup_date
    SETTINGS index_granularity = 8192
    """

    print("Creating ClickHouse table if not exists: gold_daily_trips")
    execute_clickhouse_query(query)


def create_gold_hourly_trips_table() -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_hourly_trips
    (
        pickup_date Date,
        pickup_hour UInt8,
        trips_count UInt32,
        total_revenue Float64,
        avg_check Float64,
        avg_trip_distance Float64,
        avg_trip_duration_minutes Float64,
        year String,
        month String,
        gold_load_timestamp DateTime
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(pickup_date)
    ORDER BY (pickup_date, pickup_hour)
    SETTINGS index_granularity = 8192
    """

    print("Creating ClickHouse table if not exists: gold_hourly_trips")
    execute_clickhouse_query(query)


def create_gold_payment_type_stats_table() -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_payment_type_stats
    (
        pickup_date Date,
        payment_type Int64,
        payment_type_name String,
        trips_count Int64,
        total_revenue Float64,
        avg_check Float64,
        total_tips Float64,
        avg_tip Float64,
        tips_share_from_revenue Float64,
        year String,
        month String,
        gold_load_timestamp DateTime64(6)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(pickup_date)
    ORDER BY (pickup_date, payment_type)
    SETTINGS index_granularity = 8192
    """

    print("Creating ClickHouse table if not exists: gold_payment_type_stats")
    execute_clickhouse_query(query)


def create_gold_location_pair_stats_table() -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_location_pair_stats
    (
        pickup_date Date,
        pickup_location_id Int32,
        pickup_borough Nullable(String),
        pickup_zone Nullable(String),
        pickup_service_zone Nullable(String),
        dropoff_location_id Int32,
        dropoff_borough Nullable(String),
        dropoff_zone Nullable(String),
        dropoff_service_zone Nullable(String),
        trips_count Int64,
        total_revenue Float64,
        avg_check Float64,
        avg_trip_distance Float64,
        avg_trip_duration_minutes Float64,
        year String,
        month String,
        gold_load_timestamp DateTime64(6)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(pickup_date)
    ORDER BY (pickup_date, pickup_location_id, dropoff_location_id)
    SETTINGS index_granularity = 8192
    """

    print("Creating ClickHouse table if not exists: gold_location_pair_stats")
    execute_clickhouse_query(query)


def main() -> None:
    create_database()
    create_gold_daily_trips_table()
    create_gold_hourly_trips_table()
    create_gold_payment_type_stats_table()
    create_gold_location_pair_stats_table()

    print("ClickHouse gold tables created successfully")


if __name__ == "__main__":
    main()