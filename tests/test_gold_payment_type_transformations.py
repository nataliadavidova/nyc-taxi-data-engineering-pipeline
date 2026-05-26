from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gold_payment_type_stats import build_gold_payment_type_stats


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_payment_type_transformations")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture
def silver_payment_schema():
    return StructType(
        [
            StructField("pickup_date", DateType(), False),
            StructField("trip_type", StringType(), False),
            StructField("payment_type", LongType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("tip_amount", DoubleType(), False),
        ]
    )


def test_build_gold_payment_type_stats_aggregates_payment_metrics(
    spark,
    silver_payment_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 1, 10.0, 2.0),
            (date(2024, 1, 15), "short", 1, 20.0, 4.0),
            (date(2024, 1, 15), "medium", 2, 30.0, 0.0),
        ],
        schema=silver_payment_schema,
    )

    gold_df = build_gold_payment_type_stats(silver_df)

    rows = {
        (row["pickup_date"].isoformat(), row["trip_type"], row["payment_type"]): row
        for row in gold_df.collect()
    }

    credit_short = rows[("2024-01-15", "short", 1)]
    cash_medium = rows[("2024-01-15", "medium", 2)]

    assert credit_short["payment_type_name"] == "Credit card"
    assert credit_short["trips_count"] == 2
    assert credit_short["total_revenue"] == 30.0
    assert credit_short["avg_check"] == 15.0
    assert credit_short["total_tips"] == 6.0
    assert credit_short["avg_tip"] == 3.0
    assert credit_short["tips_share_from_revenue"] == pytest.approx(0.2)
    assert credit_short["year"] == "2024"
    assert credit_short["month"] == "01"

    assert cash_medium["payment_type_name"] == "Cash"
    assert cash_medium["trips_count"] == 1
    assert cash_medium["total_revenue"] == 30.0
    assert cash_medium["avg_check"] == 30.0
    assert cash_medium["total_tips"] == 0.0
    assert cash_medium["avg_tip"] == 0.0
    assert cash_medium["tips_share_from_revenue"] == 0.0


def test_build_gold_payment_type_stats_maps_payment_type_names(
    spark,
    silver_payment_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 1, 10.0, 1.0),
            (date(2024, 1, 15), "short", 2, 10.0, 1.0),
            (date(2024, 1, 15), "short", 3, 10.0, 1.0),
            (date(2024, 1, 15), "short", 4, 10.0, 1.0),
            (date(2024, 1, 15), "short", 5, 10.0, 1.0),
            (date(2024, 1, 15), "short", 6, 10.0, 1.0),
            (date(2024, 1, 15), "short", 99, 10.0, 1.0),
        ],
        schema=silver_payment_schema,
    )

    gold_df = build_gold_payment_type_stats(silver_df)

    payment_names = {
        row["payment_type"]: row["payment_type_name"]
        for row in gold_df.collect()
    }

    assert payment_names[1] == "Credit card"
    assert payment_names[2] == "Cash"
    assert payment_names[3] == "No charge"
    assert payment_names[4] == "Dispute"
    assert payment_names[5] == "Unknown"
    assert payment_names[6] == "Voided trip"
    assert payment_names[99] == "Other"


def test_build_gold_payment_type_stats_handles_zero_revenue(
    spark,
    silver_payment_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 1, 0.0, 2.0),
            (date(2024, 1, 15), "short", 1, 0.0, 3.0),
        ],
        schema=silver_payment_schema,
    )

    gold_df = build_gold_payment_type_stats(silver_df)

    row = gold_df.collect()[0]

    assert row["total_revenue"] == 0.0
    assert row["total_tips"] == 5.0
    assert row["tips_share_from_revenue"] == 0.0


def test_build_gold_payment_type_stats_output_columns(
    spark,
    silver_payment_schema,
):
    silver_df = spark.createDataFrame(
        [
            (date(2024, 1, 15), "short", 1, 10.0, 2.0),
        ],
        schema=silver_payment_schema,
    )

    gold_df = build_gold_payment_type_stats(silver_df)

    expected_columns = [
        "pickup_date",
        "trip_type",
        "payment_type",
        "payment_type_name",
        "trips_count",
        "total_revenue",
        "avg_check",
        "total_tips",
        "avg_tip",
        "tips_share_from_revenue",
        "year",
        "month",
        "gold_load_timestamp",
    ]

    assert gold_df.columns == expected_columns