from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from check_yellow_taxi_quality import (
    REQUIRED_NOT_NULL_COLUMNS,
    build_silver_quality_expressions,
    validate_silver_quality_counts,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_check_yellow_taxi_quality")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture
def silver_quality_schema():
    return StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_date", DateType(), True),
            StructField("pickup_hour", IntegerType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("payment_type", LongType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )


def make_quality_counts(**overrides):
    values = {
        "silver_count": 100,
        "outside_month_count": 0,
        "invalid_payment_type_count": 0,
        "invalid_pickup_location_count": 0,
        "invalid_dropoff_location_count": 0,
        "invalid_pickup_hour_count": 0,
        "invalid_distance_count": 0,
        "invalid_amount_count": 0,
    }

    for column_name in REQUIRED_NOT_NULL_COLUMNS:
        values[f"null_{column_name}"] = 0

    values.update(overrides)

    return Row(**values)


def test_build_silver_quality_expressions_counts_all_issues(
    spark,
    silver_quality_schema,
):
    silver_df = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 10, 30, 0),
                date(2024, 1, 15),
                10,
                100,
                200,
                1,
                3.5,
                20.0,
            ),
            (
                datetime(2024, 2, 1, 0, 0, 0),
                datetime(2024, 2, 1, 0, 30, 0),
                date(2024, 2, 1),
                24,
                0,
                -1,
                99,
                0.0,
                0.0,
            ),
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
        schema=silver_quality_schema,
    )

    expressions = build_silver_quality_expressions(
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    quality_counts = silver_df.agg(*expressions).collect()[0]

    assert quality_counts["silver_count"] == 3
    assert quality_counts["outside_month_count"] == 1
    assert quality_counts["invalid_payment_type_count"] == 2
    assert quality_counts["invalid_pickup_location_count"] == 2
    assert quality_counts["invalid_dropoff_location_count"] == 2
    assert quality_counts["invalid_pickup_hour_count"] == 2
    assert quality_counts["invalid_distance_count"] == 1
    assert quality_counts["invalid_amount_count"] == 1

    assert quality_counts["null_tpep_pickup_datetime"] == 1
    assert quality_counts["null_tpep_dropoff_datetime"] == 1
    assert quality_counts["null_pickup_date"] == 1
    assert quality_counts["null_pickup_hour"] == 1
    assert quality_counts["null_PULocationID"] == 1
    assert quality_counts["null_DOLocationID"] == 1
    assert quality_counts["null_payment_type"] == 1
    assert quality_counts["null_trip_distance"] == 1
    assert quality_counts["null_total_amount"] == 1


def test_validate_silver_quality_counts_passes_for_valid_counts():
    validate_silver_quality_counts(
        bronze_count=100,
        silver_quality_counts=make_quality_counts(silver_count=90),
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )


def test_validate_silver_quality_counts_fails_for_empty_bronze():
    with pytest.raises(ValueError, match="Bronze layer is empty"):
        validate_silver_quality_counts(
            bronze_count=0,
            silver_quality_counts=make_quality_counts(silver_count=90),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_empty_silver():
    with pytest.raises(ValueError, match="Silver layer is empty"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(silver_count=0),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_too_many_removed_rows():
    with pytest.raises(ValueError, match="Too many rows were removed"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(silver_count=60),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_outside_month_rows():
    with pytest.raises(ValueError, match="outside expected pickup date range"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                outside_month_count=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_null_required_column():
    with pytest.raises(ValueError, match="Column pickup_date contains NULL values"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                null_pickup_date=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_invalid_payment_type():
    with pytest.raises(ValueError, match="invalid payment_type"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                invalid_payment_type_count=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_invalid_pickup_location():
    with pytest.raises(ValueError, match="invalid PULocationID"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                invalid_pickup_location_count=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_invalid_dropoff_location():
    with pytest.raises(ValueError, match="invalid DOLocationID"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                invalid_dropoff_location_count=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_invalid_pickup_hour():
    with pytest.raises(ValueError, match="invalid pickup_hour"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                invalid_pickup_hour_count=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_fails_for_invalid_distance():
    with pytest.raises(ValueError, match="trip_distance <= 0"):
        validate_silver_quality_counts(
            bronze_count=100,
            silver_quality_counts=make_quality_counts(
                silver_count=90,
                invalid_distance_count=1,
            ),
            month_start="2024-01-01",
            next_month_start="2024-02-01",
        )


def test_validate_silver_quality_counts_warns_but_does_not_fail_for_zero_amount():
    validate_silver_quality_counts(
        bronze_count=100,
        silver_quality_counts=make_quality_counts(
            silver_count=90,
            invalid_amount_count=2,
        ),
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )