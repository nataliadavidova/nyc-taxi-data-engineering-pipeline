"""
Data Quality check for NYC Taxi pipeline.

Проверяет качество данных после Silver-слоя:
1. Bronze не пустой.
2. Silver не пустой.
3. Silver не потерял слишком много строк относительно Bronze.
4. В ключевых колонках Silver нет NULL.
5. Нет некорректных расстояний.
6. Даты поездок соответствуют нужному месяцу.
7. payment_type заполнен и входит в ожидаемый диапазон.
8. PULocationID и DOLocationID заполнены и положительные.
9. pickup_hour заполнен и находится в диапазоне 0–23.

Оптимизационные решения:
1. Все проверки по silver_df собраны в один aggregate.
   Почему:
   - раньше почти каждая проверка делала отдельный .count();
   - каждый .count() — это отдельный Spark action;
   - Spark много раз перечитывал silver parquet из Object Storage;
   - теперь Silver читается для quality checks один раз.

2. bronze_count оставлен отдельным.
   Почему:
   - Bronze и Silver — разные datasets;
   - нам нужно сравнить количество строк Bronze и Silver;
   - поэтому один отдельный count() по Bronze здесь нормален.

3. Не используем cache/persist.
   Почему:
   - после оптимизации silver_df используется для проверок одним aggregate;
   - значит, кэшировать его нет смысла;
   - cache/persist добавил бы лишнее давление на память/диск.

4. Добавлен try/finally.
   Почему:
   - SparkSession должен закрываться даже если какая-то DQ-проверка упадёт;
   - это production-like поведение для Airflow/Spark job.
"""

import argparse
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    count as spark_count,
    lit,
    sum as spark_sum,
    when,
)

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    bronze_yellow_path,
    get_month_boundaries,
    silver_yellow_path,
    validate_config,
)


VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]

REQUIRED_NOT_NULL_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "trip_distance",
    "total_amount",
]


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    Здесь добавлены те же S3A-настройки, которые используются в основных Spark jobs.
    Это делает job более консистентным с bronze/silver/gold jobs.
    """

    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_yellow_data_quality")
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


def build_silver_quality_expressions(
    month_start: str,
    next_month_start: str,
    required_not_null_columns: List[str] = REQUIRED_NOT_NULL_COLUMNS,
) -> List[Column]:
    """
    Build aggregate expressions for Silver quality checks.

    Main optimization:
    instead of running many silver_df.filter(...).count() actions, all Silver
    checks are calculated with one silver_df.agg(...).collect() action.

    This function only builds Spark expressions. It does not trigger execution.
    """

    null_check_expressions = [
        spark_sum(
            when(col(column_name).isNull(), 1).otherwise(0)
        )
        .cast("long")
        .alias(f"null_{column_name}")
        for column_name in required_not_null_columns
    ]

    return [
        spark_count("*").cast("long").alias("silver_count"),
        spark_sum(
            when(
                (col("pickup_date") < lit(month_start).cast("date"))
                | (col("pickup_date") >= lit(next_month_start).cast("date")),
                1,
            ).otherwise(0)
        ).cast("long").alias("outside_month_count"),
        spark_sum(
            when(
                col("payment_type").isNull()
                | (~col("payment_type").isin(VALID_PAYMENT_TYPES)),
                1,
            ).otherwise(0)
        ).cast("long").alias("invalid_payment_type_count"),
        spark_sum(
            when(col("PULocationID").isNull() | (col("PULocationID") <= 0), 1)
            .otherwise(0)
        ).cast("long").alias("invalid_pickup_location_count"),
        spark_sum(
            when(col("DOLocationID").isNull() | (col("DOLocationID") <= 0), 1)
            .otherwise(0)
        ).cast("long").alias("invalid_dropoff_location_count"),
        spark_sum(
            when(
                col("pickup_hour").isNull()
                | (col("pickup_hour") < 0)
                | (col("pickup_hour") > 23),
                1,
            ).otherwise(0)
        ).cast("long").alias("invalid_pickup_hour_count"),
        spark_sum(
            when(col("trip_distance") <= 0, 1).otherwise(0)
        ).cast("long").alias("invalid_distance_count"),
        spark_sum(
            when(col("total_amount") <= 0, 1).otherwise(0)
        ).cast("long").alias("invalid_amount_count"),
        *null_check_expressions,
    ]


def validate_silver_quality_counts(
    bronze_count: int,
    silver_quality_counts,
    month_start: str,
    next_month_start: str,
    required_not_null_columns: List[str] = REQUIRED_NOT_NULL_COLUMNS,
) -> None:
    """
    Validate aggregated Silver quality check results.

    silver_quality_counts is a single Row collected from Spark.
    All checks in this function are driver-side checks and do not trigger
    additional Spark actions.
    """

    print(f"Bronze rows: {bronze_count}")

    if bronze_count == 0:
        raise ValueError("DQ FAILED: Bronze layer is empty")

    silver_count = int(silver_quality_counts["silver_count"])
    outside_month_count = int(silver_quality_counts["outside_month_count"])
    invalid_payment_type_count = int(
        silver_quality_counts["invalid_payment_type_count"]
    )
    invalid_pickup_location_count = int(
        silver_quality_counts["invalid_pickup_location_count"]
    )
    invalid_dropoff_location_count = int(
        silver_quality_counts["invalid_dropoff_location_count"]
    )
    invalid_pickup_hour_count = int(
        silver_quality_counts["invalid_pickup_hour_count"]
    )
    invalid_distance_count = int(silver_quality_counts["invalid_distance_count"])
    invalid_amount_count = int(silver_quality_counts["invalid_amount_count"])

    print(f"Silver rows: {silver_count}")
    print(f"Silver rows outside expected month: {outside_month_count}")

    if silver_count == 0:
        raise ValueError("DQ FAILED: Silver layer is empty")

    min_expected_silver_rows = bronze_count * 0.7

    if silver_count < min_expected_silver_rows:
        raise ValueError(
            f"DQ FAILED: Too many rows were removed. "
            f"Bronze={bronze_count}, Silver={silver_count}"
        )

    if outside_month_count > 0:
        raise ValueError(
            f"Silver contains {outside_month_count} rows outside "
            f"expected pickup date range [{month_start}, {next_month_start})"
        )

    for column_name in required_not_null_columns:
        null_count = int(silver_quality_counts[f"null_{column_name}"])
        print(f"NULL count in {column_name}: {null_count}")

        if null_count > 0:
            raise ValueError(
                f"DQ FAILED: Column {column_name} contains NULL values"
            )

    print(f"Invalid payment_type count: {invalid_payment_type_count}")

    if invalid_payment_type_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid payment_type found: "
            f"{invalid_payment_type_count}"
        )

    print(f"Invalid PULocationID count: {invalid_pickup_location_count}")

    if invalid_pickup_location_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid PULocationID found: "
            f"{invalid_pickup_location_count}"
        )

    print(f"Invalid DOLocationID count: {invalid_dropoff_location_count}")

    if invalid_dropoff_location_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid DOLocationID found: "
            f"{invalid_dropoff_location_count}"
        )

    print(f"Invalid pickup_hour count: {invalid_pickup_hour_count}")

    if invalid_pickup_hour_count > 0:
        raise ValueError(
            f"DQ FAILED: invalid pickup_hour found: "
            f"{invalid_pickup_hour_count}"
        )

    print(f"Invalid trip_distance count: {invalid_distance_count}")

    if invalid_distance_count > 0:
        raise ValueError(
            f"DQ FAILED: trip_distance <= 0 found: {invalid_distance_count}"
        )

    print(f"Invalid total_amount count: {invalid_amount_count}")

    if invalid_amount_count > 0:
        print(
            f"DQ WARNING: total_amount <= 0 found: {invalid_amount_count}. "
            "These rows are allowed because NYC Taxi data may contain refunds, "
            "cancellations, or fare corrections."
        )

    print("DQ PASSED: Silver data quality checks completed successfully")


def main(year_arg: str, month_arg: str) -> None:
    spark = create_spark_session()

    try:
        bronze_path = bronze_yellow_path(year_arg, month_arg)
        silver_path = silver_yellow_path(year_arg, month_arg)

        print(f"Reading Bronze from: {bronze_path}")
        bronze_df = spark.read.parquet(bronze_path)

        print(f"Reading Silver from: {silver_path}")
        silver_df = spark.read.parquet(silver_path)

        month_start, next_month_start = get_month_boundaries(year_arg, month_arg)

        # ======================
        # BRONZE ROW COUNT
        # ======================

        # This is a separate action on bronze_df.
        #
        # Why we keep it:
        # - Bronze and Silver are different parquet datasets;
        # - row-loss validation requires bronze_count;
        # - this count() cannot be merged with the aggregate over silver_df.
        bronze_count = bronze_df.count()

        # ======================
        # SILVER QUALITY CHECKS
        # ======================

        # Build all Silver quality expressions and run one aggregate action.
        quality_expressions = build_silver_quality_expressions(
            month_start=month_start,
            next_month_start=next_month_start,
        )

        silver_quality_counts = silver_df.agg(*quality_expressions).collect()[0]

        validate_silver_quality_counts(
            bronze_count=bronze_count,
            silver_quality_counts=silver_quality_counts,
            month_start=month_start,
            next_month_start=next_month_start,
        )


    finally:
        # SparkSession закрываем всегда.
        # Если любая DQ-проверка упадёт с ValueError,
        # Spark всё равно корректно остановится.
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year_arg=args.year, month_arg=args.month)