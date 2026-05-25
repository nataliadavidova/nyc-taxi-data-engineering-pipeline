from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from silver_yellow_taxi import (
    add_dq_columns,
    build_bad_condition,
    build_silver_dataframe,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_transformations")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture
def taxi_schema():
    return StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
        ]
    )


def test_add_dq_columns_marks_valid_record_as_clean(spark, taxi_schema):
    df = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 10, 30, 0),
                3.5,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            )
        ],
        schema=taxi_schema,
    )

    dq_df = add_dq_columns(
        df=df,
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    row = dq_df.collect()[0]

    assert row["trip_duration_minutes"] == 30.0
    assert row["dq_null_pickup"] is False
    assert row["dq_outside_month"] is False
    assert row["dq_null_dropoff"] is False
    assert row["dq_wrong_time"] is False
    assert row["dq_bad_distance"] is False
    assert row["dq_bad_fare"] is False
    assert row["dq_bad_total"] is False
    assert row["dq_bad_passenger"] is False
    assert row["dq_bad_payment_type"] is False
    assert row["dq_bad_pickup_location"] is False
    assert row["dq_bad_dropoff_location"] is False
    assert row["dq_bad_duration"] is False
    assert row["dq_outlier_distance"] is False


def test_add_dq_columns_marks_invalid_records(spark, taxi_schema):
    df = spark.createDataFrame(
        [
            (
                None,
                datetime(2024, 1, 15, 10, 30, 0),
                3.5,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            ),
            (
                datetime(2024, 2, 1, 0, 0, 0),
                datetime(2024, 2, 1, 0, 30, 0),
                3.5,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            ),
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 9, 59, 0),
                3.5,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            ),
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 10, 30, 0),
                0.0,
                -1.0,
                -5.0,
                0,
                99,
                0,
                0,
            ),
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 16, 12, 1, 0),
                101.0,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            ),
        ],
        schema=taxi_schema,
    )

    dq_df = add_dq_columns(
        df=df,
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    rows = dq_df.collect()

    assert rows[0]["dq_null_pickup"] is True

    assert rows[1]["dq_outside_month"] is True

    assert rows[2]["dq_wrong_time"] is True
    assert rows[2]["dq_bad_duration"] is True

    assert rows[3]["dq_bad_distance"] is True
    assert rows[3]["dq_bad_fare"] is True
    assert rows[3]["dq_bad_total"] is True
    assert rows[3]["dq_bad_passenger"] is True
    assert rows[3]["dq_bad_payment_type"] is True
    assert rows[3]["dq_bad_pickup_location"] is True
    assert rows[3]["dq_bad_dropoff_location"] is True

    assert rows[4]["dq_bad_duration"] is True
    assert rows[4]["dq_outlier_distance"] is True


def test_build_bad_condition_filters_bad_records(spark, taxi_schema):
    df = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 10, 30, 0),
                3.5,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            ),
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 10, 30, 0),
                0.0,
                15.0,
                20.0,
                1,
                1,
                100,
                200,
            ),
        ],
        schema=taxi_schema,
    )

    dq_df = add_dq_columns(
        df=df,
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    dq_cols = [column_name for column_name in dq_df.columns if column_name.startswith("dq_")]
    bad_condition = build_bad_condition(dq_cols)

    clean_count = dq_df.filter(~bad_condition).count()
    bad_count = dq_df.filter(bad_condition).count()

    assert clean_count == 1
    assert bad_count == 1


def test_build_silver_dataframe_adds_analytical_columns_and_removes_dq_columns(
    spark,
    taxi_schema,
):
    df = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 15, 8, 45, 0),
                datetime(2024, 1, 15, 9, 0, 0),
                1.5,
                10.0,
                12.0,
                1,
                1,
                100,
                200,
            ),
            (
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 10, 30, 0),
                5.0,
                20.0,
                25.0,
                1,
                2,
                101,
                201,
            ),
            (
                datetime(2024, 1, 15, 12, 0, 0),
                datetime(2024, 1, 15, 13, 0, 0),
                12.0,
                40.0,
                50.0,
                1,
                1,
                102,
                202,
            ),
        ],
        schema=taxi_schema,
    )

    dq_df = add_dq_columns(
        df=df,
        month_start="2024-01-01",
        next_month_start="2024-02-01",
    )

    dq_cols = [column_name for column_name in dq_df.columns if column_name.startswith("dq_")]
    bad_condition = build_bad_condition(dq_cols)

    clean_df = dq_df.filter(~bad_condition)

    silver_df = build_silver_dataframe(
        clean_df=clean_df,
        dq_cols=dq_cols,
    )

    rows = silver_df.orderBy("trip_distance").collect()

    assert rows[0]["pickup_date"].isoformat() == "2024-01-15"
    assert rows[0]["pickup_hour"] == 8
    assert rows[0]["pickup_month"] == "2024-01"
    assert rows[0]["trip_type"] == "short"

    assert rows[1]["trip_type"] == "medium"
    assert rows[2]["trip_type"] == "long"

    assert "silver_load_timestamp" in silver_df.columns

    for dq_col in dq_cols:
        assert dq_col not in silver_df.columns


def test_build_bad_condition_fails_without_dq_columns():
    with pytest.raises(ValueError, match="No data quality columns found"):
        build_bad_condition([])