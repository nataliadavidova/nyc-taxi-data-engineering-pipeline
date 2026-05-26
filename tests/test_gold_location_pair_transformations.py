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

from gold_location_pair_stats import build_gold_location_pair_stats


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_location_pair_transformations")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture
def silver_location_schema():
    return StructType(
        [
            StructField("pickup_date", DateType(), False),
            StructField("trip_type", StringType(), False),
            StructField("PULocationID", IntegerType(), False),
            StructField("DOLocationID", IntegerType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("trip_distance", DoubleType(), False),
            StructField("trip_duration_minutes", DoubleType(), False),
        ]
    )


@pytest.fixture
def zones_schema():
    return StructType(
        [
            StructField("LocationID", StringType(), False),
            StructField("Borough", StringType(), False),
            StructField("Zone", StringType(), False),
            StructField("service_zone", StringType(), False),
        ]
    )


@pytest.fixture
def zones_df(spark, zones_schema):
    return spark.createDataFrame(
        [
            ("100", "Manhattan", "Pickup Zone", "Yellow Zone"),
            ("200", "Queens", "Dropoff Zone", "Boro Zone"),
            ("101", "Brooklyn", "Another Pickup Zone", "Boro Zone"),
            ("201", "Manhattan", "Another Dropoff Zone", "Yellow Zone"),
        ],
        schema=zones_schema,
    )


def test_build_gold_location_pair_stats_aggregates_route_metrics(
    spark,
    silver_location_schema,
    zones_df,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 100, 200, 10.0, 1.5, 10.0),
            (date(2024, 1, 15), "short", 100, 200, 20.0, 2.0, 15.0),
            (date(2024, 1, 15), "medium", 101, 201, 30.0, 5.0, 30.0),
        ],
        schema=silver_location_schema,
    )

    gold_df = build_gold_location_pair_stats(
        silver_df=silver_df,
        zones_df=zones_df,
    )

    rows = {
        (
            row["pickup_date"].isoformat(),
            row["trip_type"],
            row["pickup_location_id"],
            row["dropoff_location_id"],
        ): row
        for row in gold_df.collect()
    }

    short_route = rows[("2024-01-15", "short", 100, 200)]
    medium_route = rows[("2024-01-15", "medium", 101, 201)]

    assert short_route["trips_count"] == 2
    assert short_route["total_revenue"] == 30.0
    assert short_route["avg_check"] == 15.0
    assert short_route["avg_trip_distance"] == 1.75
    assert short_route["avg_trip_duration_minutes"] == 12.5
    assert short_route["year"] == "2024"
    assert short_route["month"] == "01"

    assert medium_route["trips_count"] == 1
    assert medium_route["total_revenue"] == 30.0
    assert medium_route["avg_check"] == 30.0
    assert medium_route["avg_trip_distance"] == 5.0
    assert medium_route["avg_trip_duration_minutes"] == 30.0


def test_build_gold_location_pair_stats_enriches_pickup_and_dropoff_zones(
    spark,
    silver_location_schema,
    zones_df,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 100, 200, 10.0, 1.5, 10.0),
        ],
        schema=silver_location_schema,
    )

    gold_df = build_gold_location_pair_stats(
        silver_df=silver_df,
        zones_df=zones_df,
    )

    row = gold_df.collect()[0]

    assert row["pickup_location_id"] == 100
    assert row["pickup_borough"] == "Manhattan"
    assert row["pickup_zone"] == "Pickup Zone"
    assert row["pickup_service_zone"] == "Yellow Zone"

    assert row["dropoff_location_id"] == 200
    assert row["dropoff_borough"] == "Queens"
    assert row["dropoff_zone"] == "Dropoff Zone"
    assert row["dropoff_service_zone"] == "Boro Zone"


def test_build_gold_location_pair_stats_keeps_unmatched_locations_as_null(
    spark,
    silver_location_schema,
    zones_df,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 999, 888, 10.0, 1.5, 10.0),
        ],
        schema=silver_location_schema,
    )

    gold_df = build_gold_location_pair_stats(
        silver_df=silver_df,
        zones_df=zones_df,
    )

    row = gold_df.collect()[0]

    assert row["pickup_location_id"] == 999
    assert row["dropoff_location_id"] == 888
    assert row["pickup_borough"] is None
    assert row["pickup_zone"] is None
    assert row["pickup_service_zone"] is None
    assert row["dropoff_borough"] is None
    assert row["dropoff_zone"] is None
    assert row["dropoff_service_zone"] is None


def test_build_gold_location_pair_stats_required_output_columns_exist(
    spark,
    silver_location_schema,
    zones_df,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 100, 200, 10.0, 1.5, 10.0),
        ],
        schema=silver_location_schema,
    )

    gold_df = build_gold_location_pair_stats(
        silver_df=silver_df,
        zones_df=zones_df,
    )

    expected_columns = {
        "pickup_date",
        "trip_type",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
        "trips_count",
        "total_revenue",
        "avg_check",
        "avg_trip_distance",
        "avg_trip_duration_minutes",
        "year",
        "month",
        "gold_load_timestamp",
    }

    assert expected_columns.issubset(set(gold_df.columns))