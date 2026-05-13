from pyspark.sql import SparkSession

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
        .appName("check_gold_schema")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .getOrCreate()
    )


spark = create_spark_session()

path = (
    f"s3a://{BUCKET_NAME}/nyc_taxi/gold/yellow/daily_trips/"
    "year=2024/month=01"
)

df = spark.read.parquet(path)

df.printSchema()

df.show(5, truncate=False)

path = (
    f"s3a://{BUCKET_NAME}/nyc_taxi/gold/yellow/payment_type_stats/"
    "year=2024/month=01"
)

df = spark.read.parquet(path)

df.printSchema()

df.show(5, truncate=False)

path = (
    f"s3a://{BUCKET_NAME}/nyc_taxi/gold/yellow/location_pair_stats/"
    "year=2024/month=01"
)

df = spark.read.parquet(path)

df.printSchema()

df.show(5, truncate=False)

spark.stop()