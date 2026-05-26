"""
Gold job: pickup/dropoff location pair statistics.

Что делает:
1. Читает Silver layer для конкретного года и месяца.
2. Агрегирует поездки до уровня:
   - pickup_date;
   - trip_type;
   - PULocationID;
   - DOLocationID.
3. Считает route-level метрики:
   - trips_count;
   - total_revenue;
   - avg_check;
   - avg_trip_distance;
   - avg_trip_duration_minutes.
4. Обогащает pickup/dropoff location IDs читаемыми taxi zone names через lookup.
5. Записывает результат в Gold layer.

Оптимизационные решения:
1. Убрали silver_df.count().
   Почему:
   - count() — отдельный Spark action;
   - он полностью сканирует Silver parquet;
   - затем write() снова читает Silver parquet для построения gold mart;
   - проверка, что Silver не пустой, уже выполняется в check_yellow_taxi_quality.py.

2. Убрали gold_df.show().
   Почему:
   - show() тоже Spark action;
   - он заставляет Spark посчитать gold_df до записи;
   - затем write() считает gold_df повторно;
   - для production-like pipeline preview лучше делать отдельным read/query после записи.

3. Убрали orderBy(...) перед записью.
   Почему:
   - Parquet dataset не гарантирует порядок строк при последующем чтении;
   - сортировка должна выполняться в SQL/ClickHouse/Superset при использовании данных;
   - для location pair mart строк много, поэтому orderBy может добавить дорогой sort/shuffle.

4. Сохранили агрегацию до join.
   Почему:
   - silver_df большой;
   - taxi zone lookup маленький;
   - сначала агрегируем большой dataset до route-level grain;
   - потом джойним уже уменьшенный aggregated_df с маленьким lookup.

5. Сохранили broadcast для lookup.
   Почему:
   - taxi_zone_lookup.csv маленький справочник;
   - broadcast join позволяет разослать lookup на executors;
   - это дешевле, чем shuffle join большого aggregated_df со справочником.

6. Не используем cache/persist.
   Почему:
   - после удаления count() и show() остаётся один основной action: write();
   - silver_df не используется несколькими независимыми actions;
   - cache/persist здесь только добавил бы лишнее давление на память/диск.

7. Добавили try/finally.
   Почему:
   - SparkSession должен закрываться даже если чтение, join, aggregation или write упадут.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, current_timestamp, round, sum

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT,
    S3_REGION,
    gold_location_pair_stats_path,
    silver_yellow_path,
    taxi_zone_lookup_path,
    validate_config,
)


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with S3A settings.

    validate_config() проверяет обязательные переменные окружения до старта Spark job.
    """

    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_gold_location_pair_stats")
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


def build_gold_location_pair_stats(
    silver_df: DataFrame,
    zones_df: DataFrame,
) -> DataFrame:
    """
    Build the pickup/dropoff location pair Gold mart.

    The function preserves the current optimized transformation logic:
    1. Aggregate the large Silver dataset first.
    2. Prepare pickup and dropoff lookup DataFrames from the small taxi zone lookup.
    3. Enrich aggregated route-level records with pickup/dropoff zone attributes.
    4. Add year, month, and gold load timestamp.

    Why aggregation happens before the joins:
    - silver_df contains trip-level records and can be large;
    - taxi zone lookup is small;
    - aggregating first reduces the number of rows before enrichment;
    - joining the smaller aggregated dataset is cheaper.

    This function does not read or write data, so it can be tested with small
    in-memory Spark DataFrames.
    """

    aggregated_df = (
        silver_df
        .groupBy("pickup_date", "trip_type", "PULocationID", "DOLocationID")
        .agg(
            count("*").alias("trips_count"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("total_amount"), 2).alias("avg_check"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration_minutes"), 2).alias(
                "avg_trip_duration_minutes"
            ),
        )
    )

    pickup_zones_df = (
        zones_df
        .select(
            col("LocationID").cast("int").alias("pickup_location_id"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone"),
            col("service_zone").alias("pickup_service_zone"),
        )
    )

    dropoff_zones_df = (
        zones_df
        .select(
            col("LocationID").cast("int").alias("dropoff_location_id"),
            col("Borough").alias("dropoff_borough"),
            col("Zone").alias("dropoff_zone"),
            col("service_zone").alias("dropoff_service_zone"),
        )
    )

    return (
        aggregated_df
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .join(
            broadcast(pickup_zones_df),
            on="pickup_location_id",
            how="left",
        )
        .join(
            broadcast(dropoff_zones_df),
            on="dropoff_location_id",
            how="left",
        )
        .withColumn("year", col("pickup_date").substr(1, 4))
        .withColumn("month", col("pickup_date").substr(6, 2))
        .withColumn("gold_load_timestamp", current_timestamp())
    )


def main(year: str, month: str) -> None:
    spark = create_spark_session()

    try:
        silver_path = silver_yellow_path(year, month)
        gold_path = gold_location_pair_stats_path(year, month)
        lookup_path = taxi_zone_lookup_path()

        print(f"Reading silver data from: {silver_path}")
        silver_df = spark.read.parquet(silver_path)

        # ВАЖНО:
        # Не делаем silver_df.count().
        #
        # Почему:
        # - count() запускает отдельный Spark action;
        # - Spark полностью читает Silver parquet;
        # - затем write() ниже снова читает Silver parquet, чтобы построить gold mart.
        #
        # Проверка, что Silver не пустой, уже есть в:
        # jobs/check_yellow_taxi_quality.py

        print(f"Reading taxi zone lookup from: {lookup_path}")

        zones_df = (
            spark.read
            .option("header", "true")
            .csv(lookup_path)
        )

        # Сначала агрегируем большой Silver dataset.
        #
        # Это правильный порядок:
        # - silver_df содержит много строк поездок;
        # - aggregated_df уже содержит route-level статистику;
        # - после агрегации данных меньше, и join со справочником дешевле.

        gold_df = build_gold_location_pair_stats(
            silver_df=silver_df,
            zones_df=zones_df,
        )

        # ВАЖНО:
        # Не делаем gold_df.show().
        #
        # Почему:
        # - show() запускает отдельный Spark action;
        # - затем write() запускает ещё один Spark action;
        # - без cache/persist это может повторно пересчитать gold_df.
        #
        # Если нужен preview, лучше проверять результат после записи:
        # spark.read.parquet(gold_path).show(...)
        # или через ClickHouse после загрузки.

        # ВАЖНО:
        # Не делаем orderBy(...) перед записью.
        #
        # Почему:
        # - для Parquet порядок строк не является гарантированным контрактом;
        # - при следующем чтении Spark может читать part-файлы в другом порядке;
        # - для сортировки результата используем ORDER BY в ClickHouse/Superset;
        # - в этом mart строк много, поэтому global sort может быть дорогим.

        print(f"Writing gold data to: {gold_path}")

        (
            gold_df
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )

        print("Gold location pair stats job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)