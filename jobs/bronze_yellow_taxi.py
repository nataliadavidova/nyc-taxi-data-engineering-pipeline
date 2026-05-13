"""
Bronze job for NYC Yellow Taxi data.

Что делает:
1. Читает raw parquet-файлы yellow taxi из Yandex Object Storage.
2. Добавляет служебные поля загрузки.
3. Сохраняет данные в bronze layer в Yandex Object Storage.
4. Поддерживает запуск по конкретному году и месяцу.

Как запускать:
python jobs/bronze_yellow_taxi.py --year 2024 --month 01
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BUCKET_NAME,
    S3_ENDPOINT,
    S3_REGION,
    validate_config,
)

def create_spark_session() -> SparkSession:
    validate_config()

    return (
        SparkSession.builder
        .appName("nyc_taxi_bronze_yellow")
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

def main(year: str, month: str) -> None:
    spark = create_spark_session()

    input_path = (
        f"s3a://{BUCKET_NAME}/nyc_taxi/raw/yellow/"
        f"year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet"
    )

    output_path = (
        f"s3a://{BUCKET_NAME}/nyc_taxi/bronze/yellow/"
        f"year={year}/month={month}"
    )

    print(f"Reading raw data from: {input_path}")

    df = spark.read.parquet(input_path)

    bronze_df = (
        df
        .withColumn("load_timestamp", current_timestamp())
        .withColumn("source_system", lit("nyc_taxi_yellow"))
        .withColumn("source_year", lit(year))
        .withColumn("source_month", lit(month))
    )

    print("Schema:")
    bronze_df.printSchema()

    rows_count = bronze_df.count()
    print(f"Rows count: {rows_count}")

    print(f"Writing bronze data to: {output_path}")

    (
        bronze_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("Bronze job completed successfully")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)

    args = parser.parse_args()

    main(year=args.year, month=args.month)