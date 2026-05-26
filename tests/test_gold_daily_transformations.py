from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from gold_daily_trips import build_gold_daily_trips


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_daily_transformations")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture
def silver_daily_schema():
    return StructType(
        [
            StructField("pickup_date", DateType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("trip_distance", DoubleType(), False),
            StructField("trip_duration_minutes", DoubleType(), False),
            StructField("trip_type", StringType(), False),
        ]
    )


def test_build_gold_daily_trips_aggregates_daily_metrics(
    spark,
    silver_daily_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), 10.0, 1.5, 10.0, "short"),
            (date(2024, 1, 15), 20.0, 5.0, 20.0, "medium"),
            (date(2024, 1, 15), 30.0, 12.0, 30.0, "long"),
        ],
        schema=silver_daily_schema,
    )

    gold_df = build_gold_daily_trips(silver_df)

    rows = gold_df.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["pickup_date"].isoformat() == "2024-01-15"
    assert row["trips_count"] == 3
    assert row["total_revenue"] == 60.0
    assert row["avg_check"] == 20.0
    assert row["avg_trip_distance"] == pytest.approx(6.17)
    assert row["avg_trip_duration_minutes"] == 20.0
    assert row["short_trips_count"] == 1
    assert row["medium_trips_count"] == 1
    assert row["long_trips_count"] == 1
    assert row["year"] == "2024"
    assert row["month"] == "01"
    assert "gold_load_timestamp" in gold_df.columns


def test_build_gold_daily_trips_groups_by_pickup_date(
    spark,
    silver_daily_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), 10.0, 1.5, 10.0, "short"),
            (date(2024, 1, 15), 20.0, 5.0, 20.0, "medium"),
            (date(2024, 1, 16), 30.0, 12.0, 30.0, "long"),
        ],
        schema=silver_daily_schema,
    )

    gold_df = build_gold_daily_trips(silver_df)

    rows = {
        row["pickup_date"].isoformat(): row
        for row in gold_df.collect()
    }

    assert set(rows.keys()) == {"2024-01-15", "2024-01-16"}

    assert rows["2024-01-15"]["trips_count"] == 2
    assert rows["2024-01-15"]["total_revenue"] == 30.0
    assert rows["2024-01-15"]["short_trips_count"] == 1
    assert rows["2024-01-15"]["medium_trips_count"] == 1
    assert rows["2024-01-15"]["long_trips_count"] == 0

    assert rows["2024-01-16"]["trips_count"] == 1
    assert rows["2024-01-16"]["total_revenue"] == 30.0
    assert rows["2024-01-16"]["short_trips_count"] == 0
    assert rows["2024-01-16"]["medium_trips_count"] == 0
    assert rows["2024-01-16"]["long_trips_count"] == 1


def test_build_gold_daily_trips_output_columns(
    spark,
    silver_daily_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), 10.0, 1.5, 10.0, "short"),
        ],
        schema=silver_daily_schema,
    )

    gold_df = build_gold_daily_trips(silver_df)

    expected_columns = [
        "pickup_date",
        "trips_count",
        "total_revenue",
        "avg_check",
        "avg_trip_distance",
        "avg_trip_duration_minutes",
        "short_trips_count",
        "medium_trips_count",
        "long_trips_count",
        "year",
        "month",
        "gold_load_timestamp",
    ]

    assert gold_df.columns == expected_columns