# NYC Taxi Data Engineering Pipeline

End-to-end data engineering project for processing NYC Taxi trip data using a medallion architecture, Spark ETL jobs, Airflow orchestration, ClickHouse as an analytical serving layer, and Superset for BI dashboards.

## Project Overview

The goal of this project is to build a production-like batch data pipeline for NYC Taxi data.

The pipeline processes raw taxi trip data, cleans and transforms it into analytical layers, builds business-level gold marts, loads them into ClickHouse, and exposes the data through a Superset BI dashboard.

This project demonstrates core data engineering practices:

- batch data processing with Apache Spark;
- medallion architecture: bronze, silver, gold;
- workflow orchestration with Apache Airflow;
- analytical storage with ClickHouse;
- BI visualization with Apache Superset;
- Docker-based local infrastructure;
- data quality checks and schema validation.

## Tech Stack

- Python
- Apache Spark / PySpark
- Apache Airflow
- ClickHouse
- Apache Superset
- Docker / Docker Compose
- SQL
- Parquet
- S3-compatible Object Storage

## Project Structure

```text
nyc_taxi_final_project/
├── dags/
│   └── nyc_taxi_pipeline.py
├── jobs/
│   ├── config.py
│   ├── bronze_yellow_taxi.py
│   ├── silver_yellow_taxi.py
│   ├── gold_daily_trips.py
│   ├── gold_hourly_trips.py
│   ├── gold_location_pair_stats.py
│   ├── gold_payment_type_stats.py
│   ├── load_gold_daily_trips_to_clickhouse.py
│   ├── load_gold_hourly_trips_to_clickhouse.py
│   ├── load_gold_location_pair_stats_to_clickhouse.py
│   ├── load_gold_payment_type_stats_to_clickhouse.py
│   ├── check_gold_schema.py
│   └── check_yellow_taxi_quality.py
├── data/
├── docs/
├── screenshots/
├── sql/
├── superset/
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Data Pipeline

The pipeline follows a medallion architecture.

### Raw Layer

The raw layer contains the original NYC Taxi trip data in Parquet format.

### Bronze Layer

The bronze layer is created by:

```text
jobs/bronze_yellow_taxi.py
```

This layer stores the ingested source data with minimal transformations.

### Silver Layer

The silver layer is created by:

```text
jobs/silver_yellow_taxi.py
```

This layer contains cleaned and standardized taxi trip data.

Typical transformations include:

- selecting required columns;
- casting columns to proper data types;
- filtering invalid records;
- preparing data for analytical aggregation.

### Gold Layer

The gold layer contains business-ready analytical marts.

Gold jobs:

```text
jobs/gold_daily_trips.py
jobs/gold_hourly_trips.py
jobs/gold_location_pair_stats.py
jobs/gold_payment_type_stats.py
```

## Gold Marts

The following gold marts are created and loaded into ClickHouse.

### `gold_daily_trips`

Daily trip metrics.

Main use cases:

- daily trip volume;
- daily revenue;
- average check;
- average trip distance;
- average trip duration.

### `gold_hourly_trips`

Hourly demand analytics.

Main use cases:

- trips by hour of day;
- demand distribution throughout the day;
- peak hour analysis.

### `gold_location_pair_stats`

Pickup and dropoff location pair statistics.

Main use cases:

- top routes by number of trips;
- top routes by revenue;
- route-level demand analysis.

### `gold_payment_type_stats`

Payment type analytics.

Main use cases:

- trips by payment type;
- revenue by payment type;
- average check by payment type.

## ClickHouse Serving Layer

Gold marts are loaded into the ClickHouse database:

```text
nyc_taxi
```

ClickHouse tables:

```text
nyc_taxi.gold_daily_trips
nyc_taxi.gold_hourly_trips
nyc_taxi.gold_location_pair_stats
nyc_taxi.gold_payment_type_stats
```

Load jobs:

```text
jobs/load_gold_daily_trips_to_clickhouse.py
jobs/load_gold_hourly_trips_to_clickhouse.py
jobs/load_gold_location_pair_stats_to_clickhouse.py
jobs/load_gold_payment_type_stats_to_clickhouse.py
```

ClickHouse is used as an analytical serving layer for fast BI queries.

## Airflow Orchestration

The pipeline is orchestrated with Apache Airflow.

DAG file:

```text
dags/nyc_taxi_pipeline.py
```

The DAG runs the full pipeline:

```text
bronze → silver → gold marts → ClickHouse load → validation
```

## Data Quality Checks

The project includes data quality and validation jobs:

```text
jobs/check_yellow_taxi_quality.py
jobs/check_gold_schema.py
```

These checks help validate the pipeline output and ensure that the generated analytical marts are suitable for downstream reporting.

## Superset BI Dashboard

Apache Superset is used as the BI layer.

The dashboard is built on top of ClickHouse gold marts and includes:

- total trips;
- total revenue;
- average check;
- daily trips trend;
- daily revenue trend;
- trips by hour;
- trips by payment type;
- average check by payment type;
- top routes by trips;
- top routes by revenue.

Dashboard name:

```text
NYC Taxi BI Dashboard
```

## How to Run Locally

### 1. Start infrastructure

```bash
docker compose up -d
```

### 2. Open Airflow

```text
http://localhost:8080
```

Run the DAG:

```text
nyc_taxi_pipeline
```

### 3. Open ClickHouse

ClickHouse HTTP interface:

```text
http://localhost:8123
```

Example query:

```sql
SELECT *
FROM nyc_taxi.gold_daily_trips
LIMIT 10;
```

### 4. Open Superset

```text
http://localhost:8088
```

Open dashboard:

```text
NYC Taxi BI Dashboard
```

## Screenshots

### Airflow Successful Pipeline Run

![Airflow Successful Pipeline Run](screenshots/airflow_successful_run_graph.png)

### ClickHouse Gold Tables

![ClickHouse Gold Tables](screenshots/clickhouse_gold_tables.png)

### Superset BI Dashboard

![Superset BI Dashboard](screenshots/superset_dashboard.png)

## Future Improvements

Possible future improvements:

- add incremental processing;
- add more data quality checks;
- add automated tests for Spark jobs;
- add CI/CD pipeline;
- add dbt layer for analytical transformations;
- enrich location IDs with taxi zone names;
- improve dashboard filters and cross-filtering.