"""
Silver job for NYC Yellow Taxi data.

Что делает:
1. Читает bronze-слой из Yandex Object Storage.
2. Добавляет технические и аналитические поля для проверки качества.
3. Выполняет data quality checks.
4. Сохраняет bad records отдельно.
5. Формирует clean/silver dataset.
6. Проверяет качество silver-слоя перед записью.
7. Пишет silver-слой.
8. Пишет quality report.

Оптимизационные решения:
1. Не делаем отдельный df.count() в начале job.
   Почему:
   - df.count() — это отдельный Spark action.
   - Он заставляет Spark полностью прочитать monthly bronze parquet.
   - Потом следующий aggregate снова проходит по тем же данным.
   Вместо этого total_count считается внутри общего DQ aggregate.

2. DQ-метрики считаются одним aggregate.
   Почему:
   - раньше отдельные count() по каждому dq-флагу могли запускать много Spark actions;
   - теперь все dq-флаги, total_count и total_bad считаются за один проход.

3. dq_df persistится как StorageLevel.DISK_ONLY.
   Почему:
   - dq_df используется несколько раз: для bad records, clean records, silver checks и записи;
   - DISK_ONLY снижает recomputation, но не давит на JVM heap так сильно, как memory cache.

4. silver_df не persistим.
   Почему:
   - silver_df строится от уже persisted dq_df;
   - дополнительный persist silver_df может увеличить disk/memory pressure;
   - раньше это могло приводить к рискам Java heap OOM.

5. Используем try/finally.
   Почему:
   - SparkSession должен закрываться даже при ошибке;
   - persisted dq_df должен освобождаться даже при падении job.
"""

import argparse
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    count as spark_count,
    current_timestamp,
    date_format,
    hour,
    lit,
    sum as spark_sum,
    to_date,
    unix_timestamp,
    when,
)
from pyspark.storagelevel import StorageLevel

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    bad_records_yellow_path,
    bronze_yellow_path,
    get_month_boundaries,
    quality_yellow_path,
    silver_yellow_path,
    validate_config,
)


VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    validate_config() запускается до создания SparkSession, чтобы job падал сразу,
    если не заданы обязательные переменные окружения для Object Storage.
    """

    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_silver_yellow")
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


def build_bad_condition(dq_cols: list):
    """
    Build one boolean condition for all DQ checks.

    Строка считается bad record, если хотя бы один dq-флаг равен True.

    Было:
        bad_condition = None
        for c in dq_cols:
            bad_condition = col(c) if bad_condition is None else bad_condition | col(c)

    Стало:
        reduce(...)

    Почему так лучше:
    - код короче;
    - нет временного None;
    - явно видно, что мы объединяем все dq-флаги через OR.
    """

    if not dq_cols:
        raise ValueError("No data quality columns found")

    return reduce(
        lambda condition, c: condition | col(c),
        dq_cols[1:],
        col(dq_cols[0]),
    )


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    # dq_df объявляем заранее, чтобы в finally можно было безопасно вызвать unpersist().
    # Если job упадёт до создания dq_df, переменная останется None.
    dq_df = None

    try:
        bronze_path = bronze_yellow_path(year, month)
        silver_path = silver_yellow_path(year, month)
        bad_records_path = bad_records_yellow_path(year, month)
        quality_path = quality_yellow_path(year, month)

        print(f"Reading bronze: {bronze_path}")
        df = spark.read.parquet(bronze_path)

        # ВАЖНО:
        # Раньше здесь был df.count().
        #
        # total_count = df.count()
        # print(f"Total rows: {total_count}")
        #
        # Мы его убрали, потому что count() — это отдельный Spark action.
        # Для monthly parquet в Object Storage это означает лишний полный проход по данным.
        # Теперь total_count считается ниже вместе со всеми DQ-метриками одним aggregate.

        month_start, next_month_start = get_month_boundaries(year, month)

        print(f"Expected pickup date range: [{month_start}, {next_month_start})")

        # ======================
        # DATA QUALITY CHECKS
        # ======================

        # Добавляем trip_duration_minutes до DQ-флагов, потому что он нужен
        # для проверки dq_bad_duration.
        dq_df = df.withColumn(
            "trip_duration_minutes",
            (
                unix_timestamp("tpep_dropoff_datetime")
                - unix_timestamp("tpep_pickup_datetime")
            )
            / 60,
        )

        # Добавляем набор boolean dq-флагов.
        #
        # Каждый флаг отвечает за отдельное правило качества.
        # Позже мы:
        # - посчитаем количество нарушений по каждому флагу;
        # - соберём bad records;
        # - удалим bad records из silver.
        dq_df = (
            dq_df
            .withColumn("dq_null_pickup", col("tpep_pickup_datetime").isNull())
            .withColumn(
                "dq_outside_month",
                col("tpep_pickup_datetime").isNotNull()
                & (
                    (to_date("tpep_pickup_datetime") < lit(month_start).cast("date"))
                    | (
                        to_date("tpep_pickup_datetime")
                        >= lit(next_month_start).cast("date")
                    )
                ),
            )
            .withColumn("dq_null_dropoff", col("tpep_dropoff_datetime").isNull())
            .withColumn(
                "dq_wrong_time",
                col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"),
            )
            .withColumn("dq_bad_distance", col("trip_distance") <= 0)
            .withColumn("dq_bad_fare", col("fare_amount") < 0)
            .withColumn("dq_bad_total", col("total_amount") < 0)
            .withColumn(
                "dq_bad_passenger",
                col("passenger_count").isNotNull() & (col("passenger_count") <= 0),
            )
            .withColumn(
                "dq_bad_payment_type",
                col("payment_type").isNull()
                | (~col("payment_type").isin(VALID_PAYMENT_TYPES)),
            )
            .withColumn(
                "dq_bad_pickup_location",
                col("PULocationID").isNull() | (col("PULocationID") <= 0),
            )
            .withColumn(
                "dq_bad_dropoff_location",
                col("DOLocationID").isNull() | (col("DOLocationID") <= 0),
            )
            .withColumn(
                "dq_bad_duration",
                (col("trip_duration_minutes") <= 0)
                | (col("trip_duration_minutes") > 1440),
            )
            .withColumn("dq_outlier_distance", col("trip_distance") > 100)
        )

        dq_cols = [c for c in dq_df.columns if c.startswith("dq_")]

        # dq_df используется дальше несколько раз:
        # 1. для DQ aggregate;
        # 2. для bad_df.write;
        # 3. для clean_df / silver_df;
        # 4. для silver quality checks;
        # 5. для silver_df.write.
        #
        # Поэтому persist помогает не пересчитывать всю цепочку DQ-колонок каждый раз.
        #
        # Почему DISK_ONLY:
        # - monthly taxi data может быть большим;
        # - MEMORY_ONLY / MEMORY_AND_DISK могут давить на JVM heap;
        # - DISK_ONLY безопаснее для локального Docker/Spark окружения.
        dq_df = dq_df.persist(StorageLevel.DISK_ONLY)

        # ======================
        # BAD RECORDS CONDITION
        # ======================

        # bad_condition = OR по всем dq-флагам.
        # Если хотя бы один dq-флаг True, строка считается плохой.
        bad_condition = build_bad_condition(dq_cols)

        # ======================
        # DATA QUALITY AGGREGATION
        # ======================

        # ВАЖНОЕ ИЗМЕНЕНИЕ:
        # total_count теперь считается здесь же, вместе со всеми DQ-флагами.
        #
        # Это заменяет отдельный df.count() в начале job.
        #
        # Было:
        # - action 1: df.count()
        # - action 2: dq_df.agg(...).collect()
        #
        # Стало:
        # - action 1: dq_df.agg(total_count, dq flags, total_bad).collect()
        #
        # Так мы убираем один лишний полный проход по bronze dataset.
        report_agg_expressions = [
            spark_count("*").cast("long").alias("total_count"),
            *[
                spark_sum(when(col(c), 1).otherwise(0)).cast("long").alias(c)
                for c in dq_cols
            ],
        ]

        report_agg_expressions.append(
            spark_sum(when(bad_condition, 1).otherwise(0))
            .cast("long")
            .alias("total_bad")
        )

        report_counts = dq_df.agg(*report_agg_expressions).collect()[0]

        total_count = int(report_counts["total_count"])
        bad_count = int(report_counts["total_bad"])

        print(f"Total rows: {total_count}")
        print(f"Bad rows: {bad_count}")

        # ======================
        # BAD RECORDS
        # ======================

        bad_df = dq_df.filter(bad_condition)

        # dq_reason собирает список dq-флагов, которые сработали для строки.
        # Это полезно для анализа качества данных: можно понять, почему запись
        # попала в bad records.
        bad_df = bad_df.withColumn(
            "dq_reason",
            concat_ws(
                "; ",
                *[when(col(c), lit(c)) for c in dq_cols],
            ),
        )

        print(f"Writing bad records to: {bad_records_path}")
        bad_df.write.mode("overwrite").parquet(bad_records_path)

        # ======================
        # CLEAN DATA
        # ======================

        # clean_df — это все строки, где не сработал ни один dq-флаг.
        clean_df = dq_df.filter(~bad_condition)

        # silver_df — чистый слой с производными аналитическими полями.
        #
        # Здесь добавляем:
        # - pickup_date для daily/monthly analytics;
        # - pickup_hour для hourly demand analytics;
        # - pickup_month для monthly trends;
        # - trip_type для short/medium/long analysis;
        # - silver_load_timestamp для auditability.
        #
        # dq_cols удаляем, потому что DQ-флаги нужны для контроля качества,
        # но не должны попадать в чистый silver contract.
        silver_df = (
            clean_df
            .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
            .withColumn("pickup_month", date_format("tpep_pickup_datetime", "yyyy-MM"))
            .withColumn(
                "trip_type",
                when(col("trip_distance") < 2, "short")
                .when(col("trip_distance") <= 10, "medium")
                .otherwise("long"),
            )
            .withColumn("silver_load_timestamp", current_timestamp())
            .drop(*dq_cols)
        )

        # ======================
        # SILVER QUALITY CHECKS
        # ======================

        # Проверяем, что после очистки silver действительно соответствует
        # базовому контракту:
        # - pickup_date внутри ожидаемого месяца;
        # - pickup_hour в диапазоне 0..23;
        # - silver_count считаем для логирования и quality report.
        #
        # Это отдельный action, но он работает от persisted dq_df,
        # поэтому не должен заново читать bronze parquet из Object Storage.
        silver_quality_counts = silver_df.agg(
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
                    col("pickup_hour").isNull()
                    | (col("pickup_hour") < 0)
                    | (col("pickup_hour") > 23),
                    1,
                ).otherwise(0)
            ).cast("long").alias("invalid_pickup_hour_count"),
        ).collect()[0]

        silver_count = int(silver_quality_counts["silver_count"])
        outside_month_count = int(silver_quality_counts["outside_month_count"])
        invalid_pickup_hour_count = int(
            silver_quality_counts["invalid_pickup_hour_count"]
        )

        if outside_month_count > 0:
            raise ValueError(
                f"Silver contains {outside_month_count} rows outside "
                f"expected pickup date range [{month_start}, {next_month_start})"
            )

        if invalid_pickup_hour_count > 0:
            raise ValueError(
                f"Silver contains {invalid_pickup_hour_count} rows "
                "with invalid pickup_hour"
            )

        print(f"Clean rows: {silver_count}")
        print(f"Removed rows: {total_count - silver_count}")

        print(f"Writing silver data to: {silver_path}")
        silver_df.write.mode("overwrite").parquet(silver_path)

        # ======================
        # QUALITY REPORT
        # ======================

        # Собираем маленький quality report из уже посчитанных report_counts.
        #
        # Важно:
        # здесь мы НЕ запускаем новые count() по каждому dq-флагу.
        # Все значения берутся из одного общего aggregate выше.
        report_data = []

        for c in dq_cols:
            failed = int(report_counts[c])
            share = failed / total_count if total_count else 0.0
            report_data.append((c, failed, total_count, share))

        total_bad_share = bad_count / total_count if total_count else 0.0
        report_data.append(("total_bad", bad_count, total_count, total_bad_share))

        report_df = (
            spark.createDataFrame(
                report_data,
                ["check", "failed_rows", "total_rows", "share"],
            )
            .withColumn("created_at", current_timestamp())
        )

        # report_df маленький: в нём одна строка на DQ-check.
        # show() здесь не создаёт большой нагрузки и полезен в Airflow logs.
        report_df.show(truncate=False)

        print(f"Writing quality report to: {quality_path}")
        report_df.write.mode("overwrite").parquet(quality_path)

        print("Silver job DONE ✅")

    finally:
        # ВАЖНО:
        # Если dq_df был persisted, освобождаем его даже при ошибке.
        # Это особенно важно для Airflow/Spark jobs, чтобы не оставлять
        # лишние persisted blocks между этапами.
        if dq_df is not None:
            dq_df.unpersist()

        # SparkSession закрываем всегда.
        # Если job упадёт на read/write/validation, Spark всё равно остановится.
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)
