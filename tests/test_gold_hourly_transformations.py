from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gold_hourly_trips import build_gold_hourly_trips


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_hourly_transformations")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture
def silver_hourly_schema():
    return StructType(
        [
            StructField("pickup_date", DateType(), False),
            StructField("pickup_hour", IntegerType(), False),
            StructField("trip_type", StringType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("trip_distance", DoubleType(), False),
            StructField("trip_duration_minutes", DoubleType(), False),
        ]
    )


def test_build_gold_hourly_trips_aggregates_hourly_metrics(
    spark,
    silver_hourly_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), 10, "short", 10.0, 1.0, 10.0),
            (date(2024, 1, 15), 10, "short", 20.0, 2.0, 20.0),
            (date(2024, 1, 15), 10, "medium", 30.0, 5.0, 30.0),
        ],
        schema=silver_hourly_schema,
    )

    gold_df = build_gold_hourly_trips(silver_df)

    rows = {
        (row["pickup_date"].isoformat(), row["pickup_hour"], row["trip_type"]): row
        for row in gold_df.collect()
    }

    short_row = rows[("2024-01-15", 10, "short")]
    medium_row = rows[("2024-01-15", 10, "medium")]

    assert short_row["trips_count"] == 2
    assert short_row["total_revenue"] == 30.0
    assert short_row["avg_check"] == 15.0
    assert short_row["avg_trip_distance"] == 1.5
    assert short_row["avg_trip_duration_minutes"] == 15.0
    assert short_row["year"] == "2024"
    assert short_row["month"] == "01"

    assert medium_row["trips_count"] == 1
    assert medium_row["total_revenue"] == 30.0


def test_build_gold_hourly_trips_groups_by_date_hour_and_trip_type(
    spark,
    silver_hourly_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), 10, "short", 10.0, 1.0, 10.0),
            (date(2024, 1, 15), 11, "short", 20.0, 2.0, 20.0),
            (date(2024, 1, 16), 10, "short", 30.0, 3.0, 30.0),
        ],
        schema=silver_hourly_schema,
    )

    gold_df = build_gold_hourly_trips(silver_df)

    rows = gold_df.collect()

    assert len(rows) == 3

    keys = {
        (row["pickup_date"].isoformat(), row["pickup_hour"], row["trip_type"])
        for row in rows
    }

    assert keys == {
        ("2024-01-15", 10, "short"),
        ("2024-01-15", 11, "short"),
        ("2024-01-16", 10, "short"),
    }


def test_build_gold_hourly_trips_output_columns(
    spark,
    silver_hourly_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), 10, "short", 10.0, 1.0, 10.0),
        ],
        schema=silver_hourly_schema,
    )

    gold_df = build_gold_hourly_trips(silver_df)

    expected_columns = [
        "pickup_date",
        "pickup_hour",
        "trip_type",
        "trips_count",
        "total_revenue",
        "avg_check",
        "avg_trip_distance",
        "avg_trip_duration_minutes",
        "year",
        "month",
        "gold_load_timestamp",
    ]

    assert gold_df.columns == expected_columns