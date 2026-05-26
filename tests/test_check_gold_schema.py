from datetime import date

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from check_gold_schema import (
    assert_expected_columns,
    build_quality_expressions,
    empty_string_condition,
    validate_quality_counts,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_check_gold_schema")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_assert_expected_columns_passes_for_valid_daily_schema(spark):
    df = spark.createDataFrame(
        [
            (
                date(2024, 1, 15),
                10,
                100.0,
                10.0,
                2.5,
                15.0,
                3,
                5,
                2,
                "2024",
                "01",
                None,
            )
        ],
        schema="""
            pickup_date date,
            trips_count int,
            total_revenue double,
            avg_check double,
            avg_trip_distance double,
            avg_trip_duration_minutes double,
            short_trips_count int,
            medium_trips_count int,
            long_trips_count int,
            year string,
            month string,
            gold_load_timestamp timestamp
        """,
    )

    assert_expected_columns(df, "gold_daily_trips")


def test_assert_expected_columns_fails_for_missing_columns(spark):
    df = spark.createDataFrame(
        [
            (
                date(2024, 1, 15),
                10,
            )
        ],
        schema="""
            pickup_date date,
            trips_count int
        """,
    )

    with pytest.raises(AssertionError, match="missing expected columns"):
        assert_expected_columns(df, "gold_daily_trips")


def test_empty_string_condition_detects_null_empty_and_whitespace_values(spark):
    schema = StructType(
        [
            StructField("trip_type", StringType(), True),
        ]
    )

    df = spark.createDataFrame(
        [
            ("short",),
            ("",),
            ("   ",),
            (None,),
        ],
        schema=schema,
    )

    invalid_count = df.filter(empty_string_condition("trip_type")).count()

    assert invalid_count == 3


def test_build_quality_expressions_for_daily_table_counts_rows_and_dates(spark):
    df = spark.createDataFrame(
        [
            (date(2024, 1, 15),),
            (date(2024, 2, 1),),
            (None,),
        ],
        schema=StructType(
            [
                StructField("pickup_date", DateType(), True),
            ]
        ),
    )

    expressions = build_quality_expressions(
        table_name="gold_daily_trips",
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    quality_counts = df.agg(*expressions).collect()[0]

    assert quality_counts["rows_count"] == 3
    assert quality_counts["outside_month_count"] == 2


def test_build_quality_expressions_for_hourly_table_counts_specific_issues(spark):
    df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 10),
            (date(2024, 1, 15), "", 24),
            (date(2024, 1, 15), None, None),
            (date(2024, 2, 1), "medium", 12),
        ],
        schema=StructType(
            [
                StructField("pickup_date", DateType(), True),
                StructField("trip_type", StringType(), True),
                StructField("pickup_hour", IntegerType(), True),
            ]
        ),
    )

    expressions = build_quality_expressions(
        table_name="gold_hourly_trips",
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    quality_counts = df.agg(*expressions).collect()[0]

    assert quality_counts["rows_count"] == 4
    assert quality_counts["outside_month_count"] == 1
    assert quality_counts["empty_trip_type_count"] == 2
    assert quality_counts["invalid_pickup_hour_count"] == 2


def test_build_quality_expressions_for_payment_table_counts_specific_issues(spark):
    df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", "Credit card"),
            (date(2024, 1, 15), "", ""),
            (date(2024, 1, 15), None, None),
        ],
        schema=StructType(
            [
                StructField("pickup_date", DateType(), True),
                StructField("trip_type", StringType(), True),
                StructField("payment_type_name", StringType(), True),
            ]
        ),
    )

    expressions = build_quality_expressions(
        table_name="gold_payment_type_stats",
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    quality_counts = df.agg(*expressions).collect()[0]

    assert quality_counts["rows_count"] == 3
    assert quality_counts["outside_month_count"] == 0
    assert quality_counts["empty_trip_type_count"] == 2
    assert quality_counts["empty_payment_type_name_count"] == 2


def test_build_quality_expressions_for_location_table_counts_specific_issues(spark):
    df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", "Pickup Zone", "Dropoff Zone"),
            (date(2024, 1, 15), "", "", ""),
            (date(2024, 1, 15), None, None, None),
        ],
        schema=StructType(
            [
                StructField("pickup_date", DateType(), True),
                StructField("trip_type", StringType(), True),
                StructField("pickup_zone", StringType(), True),
                StructField("dropoff_zone", StringType(), True),
            ]
        ),
    )

    expressions = build_quality_expressions(
        table_name="gold_location_pair_stats",
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    quality_counts = df.agg(*expressions).collect()[0]

    assert quality_counts["rows_count"] == 3
    assert quality_counts["outside_month_count"] == 0
    assert quality_counts["empty_trip_type_count"] == 2
    assert quality_counts["empty_pickup_zone_count"] == 2
    assert quality_counts["empty_dropoff_zone_count"] == 2


def test_validate_quality_counts_passes_for_valid_daily_counts():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=0,
    )

    validate_quality_counts(
        table_name="gold_daily_trips",
        quality_counts=quality_counts,
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )


def test_validate_quality_counts_fails_for_empty_table():
    quality_counts = Row(
        rows_count=0,
        outside_month_count=0,
    )

    with pytest.raises(AssertionError, match="is empty"):
        validate_quality_counts(
            table_name="gold_daily_trips",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_quality_counts_fails_for_outside_month_rows():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=1,
    )

    with pytest.raises(AssertionError, match="outside expected month range"):
        validate_quality_counts(
            table_name="gold_daily_trips",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_quality_counts_fails_for_empty_trip_type():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=0,
        empty_trip_type_count=1,
    )

    with pytest.raises(AssertionError, match="empty trip_type"):
        validate_quality_counts(
            table_name="gold_hourly_trips",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_quality_counts_fails_for_invalid_pickup_hour():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=0,
        empty_trip_type_count=0,
        invalid_pickup_hour_count=1,
    )

    with pytest.raises(AssertionError, match="invalid pickup_hour"):
        validate_quality_counts(
            table_name="gold_hourly_trips",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_quality_counts_fails_for_empty_payment_type_name():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=0,
        empty_trip_type_count=0,
        empty_payment_type_name_count=1,
    )

    with pytest.raises(AssertionError, match="empty payment_type_name"):
        validate_quality_counts(
            table_name="gold_payment_type_stats",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_quality_counts_fails_for_empty_pickup_zone():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=0,
        empty_trip_type_count=0,
        empty_pickup_zone_count=1,
        empty_dropoff_zone_count=0,
    )

    with pytest.raises(AssertionError, match="empty pickup_zone"):
        validate_quality_counts(
            table_name="gold_location_pair_stats",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_quality_counts_fails_for_empty_dropoff_zone():
    quality_counts = Row(
        rows_count=10,
        outside_month_count=0,
        empty_trip_type_count=0,
        empty_pickup_zone_count=0,
        empty_dropoff_zone_count=1,
    )

    with pytest.raises(AssertionError, match="empty dropoff_zone"):
        validate_quality_counts(
            table_name="gold_location_pair_stats",
            quality_counts=quality_counts,
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )