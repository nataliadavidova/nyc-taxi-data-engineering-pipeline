# NYC Taxi Data Engineering Pipeline

End-to-end data engineering project for processing NYC Yellow Taxi trip data using a medallion architecture, Apache Spark ETL jobs, Airflow orchestration, ClickHouse as an analytical serving layer, and Apache Superset for BI dashboards.

The pipeline processes NYC Yellow Taxi data for the full year 2024 and builds analytical marts for trips, revenue, hourly demand, payment behavior, and popular pickup/dropoff routes.

## Project Overview

The goal of this project is to build a production-like batch data pipeline for NYC Taxi data.

The pipeline ingests raw taxi trip data from S3-compatible object storage, cleans and validates the data, builds business-ready analytical marts, loads them into ClickHouse, and exposes the results through a Superset BI dashboard.

This project demonstrates core data engineering practices:

- batch data processing with Apache Spark;
- medallion architecture: raw, bronze, silver, gold;
- workflow orchestration with Apache Airflow;
- data quality checks and validation gates;
- analytical storage with ClickHouse;
- BI visualization with Apache Superset;
- Docker-based local infrastructure;
- centralized configuration, reusable path helpers, and shared ClickHouse HTTP utilities;
- full-year historical data processing;
- configured period refresh for safe month-level and interval reloads;
- raw monthly file discovery and automated new-month processing with a dedicated Airflow DAG.

## Project Highlights

- Built an end-to-end batch data engineering pipeline for full-year 2024 NYC Yellow Taxi data.
- Implemented a medallion architecture with raw, bronze, silver, and gold data layers.
- Developed PySpark ETL jobs for ingestion, cleaning, validation, feature derivation, and analytical mart creation.
- Orchestrated the full pipeline with Apache Airflow, including monthly processing, dependency management, retries, and final quality gates.
- Added a period refresh Airflow DAG for safe month-level and interval reloads without truncating the entire ClickHouse serving layer.
- Implemented `replace_period` mode with ClickHouse month deletion, monthly Spark reprocessing, ClickHouse reload, and month-level serving-layer quality checks.
- Refactored the period refresh DAG to use Airflow runtime Params / Trigger DAG config and a mapped monthly TaskGroup instead of static period constants in the DAG file.
- Added raw monthly file discovery helpers to identify new Yellow Taxi raw periods by comparing Object Storage files with periods fully loaded into all ClickHouse gold tables.
- Added `nyc_taxi_process_new_months_pipeline`, an Airflow DAG that automatically processes newly discovered raw months using dynamic task mapping and a mapped monthly TaskGroup.
- Validated the new-month processing flow on real TLC Yellow Taxi data for `2025-01`; the DAG loaded the month into all ClickHouse gold marts, passed month-level quality checks, and became a successful no-op on the second run.
- Centralized shared ClickHouse HTTP helpers in `clickhouse_utils.py` to avoid duplicated connection, authentication, timeout, error-handling, and JSON parsing logic across ClickHouse utility jobs.
- Loaded business-ready gold marts into ClickHouse as an analytical serving layer for fast BI queries.
- Built a Superset BI dashboard for executive reporting, demand analysis, payment behavior, geospatial demand patterns, and ridesharing opportunity analysis.
- Added data quality checks for Silver, Gold Object Storage, and ClickHouse layers to prevent invalid data from reaching BI reports.
- Added geospatial enrichment using taxi zone centroid coordinates for pickup and dropoff demand maps.
- Added automated tests and GitHub Actions CI to validate configuration helpers, DAG imports, Airflow task dependencies, Spark transformation logic, and quality gates.
- Optimized Spark, Object Storage, quality-check, and ClickHouse load jobs by reducing redundant actions, repeated scans, and unnecessary sorting while keeping pipeline logic unchanged.
- Added an Airflow spark_pool resource-control layer for Spark-heavy tasks across all pipeline DAGs to prevent multiple local spark-submit jobs from running at the same time in the Docker environment.
- Added Airflow task hardening with centralized retry delay, execution timeouts for Spark-heavy and lightweight tasks, and a shared failure callback that writes structured failure messages to Airflow task logs.

## Business Value

The resulting BI dashboard helps analyze NYC Yellow Taxi operations across the full year 2024.

The dashboard answers questions such as:

- how many trips were completed during a selected period;
- how total revenue changed over time;
- what hours have the highest taxi demand;
- which payment types are most commonly used;
- which pickup and dropoff zones form the most popular routes;
- how average check differs by payment type and route.

This type of pipeline can be used as a foundation for transportation analytics, demand monitoring, revenue analysis, and operational reporting.

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

## Architecture

```text
S3-compatible Object Storage
        │
        ▼
Raw Layer
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer + Data Quality Checks
        │
        ▼
Gold Analytical Marts
        │
        ▼
ClickHouse Serving Layer
        │
        ▼
Superset BI Dashboard
```

The pipeline follows a medallion architecture:

```text
raw → bronze → silver → gold → ClickHouse → Superset
```

## Project Structure

```text
nyc_taxi_final_project/
├── .github/
│   └── workflows/
│       └── ci.yml
├── dags/
│   ├── nyc_taxi_pipeline.py
│   ├── nyc_taxi_period_refresh_pipeline.py
│   └── nyc_taxi_process_new_months_pipeline.py
├── jobs/
│   ├── config.py
│   ├── period_utils.py
│   ├── period_refresh_config.py
│   ├── clickhouse_utils.py
│   ├── airflow_callbacks.py
│   ├── raw_discovery.py
│   ├── bronze_yellow_taxi.py
│   ├── silver_yellow_taxi.py
│   ├── check_yellow_taxi_quality.py
│   ├── gold_daily_trips.py
│   ├── gold_hourly_trips.py
│   ├── gold_location_pair_stats.py
│   ├── gold_payment_type_stats.py
│   ├── check_gold_schema.py
│   ├── create_clickhouse_gold_tables.py
│   ├── truncate_clickhouse_gold_tables.py
│   ├── delete_clickhouse_gold_month.py
│   ├── check_clickhouse_gold_quality.py
│   ├── check_clickhouse_gold_month_quality.py
│   ├── load_gold_daily_trips_to_clickhouse.py
│   ├── load_gold_hourly_trips_to_clickhouse.py
│   ├── load_gold_location_pair_stats_to_clickhouse.py
│   ├── load_gold_payment_type_stats_to_clickhouse.py
│   ├── load_taxi_zone_centroids_to_clickhouse.py
│   └── run_clickhouse_sql_file.py
├── tests/
│   ├── test_config.py
│   ├── test_airflow_callbacks.py
│   ├── test_dag.py
│   ├── test_period_utils.py
│   ├── test_period_refresh_config.py
│   ├── test_clickhouse_utils.py
│   ├── test_raw_discovery.py
│   ├── test_delete_clickhouse_gold_month.py
│   ├── test_check_clickhouse_gold_month_quality.py
│   ├── test_period_refresh_dag.py
│   ├── test_process_new_months_dag.py
│   ├── test_silver_transformations.py
│   ├── test_gold_daily_transformations.py
│   ├── test_gold_hourly_transformations.py
│   ├── test_gold_payment_type_transformations.py
│   ├── test_gold_location_pair_transformations.py
│   ├── test_check_yellow_taxi_quality.py
│   ├── test_check_gold_schema.py
│   └── test_check_clickhouse_gold_quality.py
├── data/
│   └── geo/
│       └── taxi_zone_centroids.csv
├── docs/
│   ├── analytics_summary.md
│   └── nyc_taxi_bi_dashboard.pdf
├── sql/
│   └── analytics/
│       ├── 01_top_pickup_zones.sql
│       ├── 02_top_dropoff_zones.sql
│       ├── 03_peak_hours.sql
│       ├── 04_trip_type_distribution.sql
│       ├── 05_peak_hours_by_trip_type.sql
│       ├── 06_top_zones_by_trip_type.sql
│       ├── 07_payment_methods_by_trip_type.sql
│       ├── 08_payment_preference_trends.sql
│       └── 09_short_trip_ridesharing_opportunities.sql
├── screenshots/
│   ├── airflow_successful_run_graph.png
│   ├── clickhouse_gold_tables.png
│   ├── superset_01_executive_overview.png
│   ├── superset_02_trip_type_and_peak_demand.png
│   ├── superset_03_heatmap_and_geo_maps.png
│   ├── superset_04_geo_demand_zones.png
│   ├── superset_05_routes_and_payment_trends.png
│   ├── superset_06_payment_analytics.png
│   └── superset_07_ridesharing_opportunities.png
├── superset/
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

## Data Source

The project uses NYC Yellow Taxi trip records in Parquet format.

The raw data is stored in S3-compatible object storage using the following structure:

```text
nyc_taxi/raw/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
nyc_taxi/raw/yellow/year=2024/month=02/yellow_tripdata_2024-02.parquet
...
nyc_taxi/raw/yellow/year=2024/month=12/yellow_tripdata_2024-12.parquet
```

A taxi zone lookup file is also used to enrich pickup and dropoff location IDs with readable zone names:

```text
nyc_taxi/raw/lookup/taxi_zone_lookup.csv
```

### Raw File Discovery

The project includes raw monthly file discovery helpers:

```text
jobs/raw_discovery.py
```

This module supports automated incremental-style processing of newly arrived monthly raw files.

It can:

- parse raw Yellow Taxi object keys from S3-compatible Object Storage;
- extract and validate `year` and `month` from raw parquet paths;
- ignore unrelated files such as lookup files, wrong taxi types, or malformed paths;
- list raw Yellow Taxi periods from Object Storage;
- list periods already fully processed in the ClickHouse serving layer;
- compare raw periods with fully processed ClickHouse periods;
- return new raw periods that still need to be processed.

Current discovery logic defines:

```text
new periods = raw periods from Object Storage - fully processed periods from ClickHouse
```

A period is considered fully processed only if it exists in all expected ClickHouse gold tables:

```text
gold_daily_trips
gold_hourly_trips
gold_location_pair_stats
gold_payment_type_stats
```

This protects the pipeline from skipping partially loaded months. For example, if a month exists in `gold_daily_trips` but is missing from another gold table, it is not treated as fully processed and can be safely picked up by the new-month processing DAG.

The discovery helper is used by:

```text
dags/nyc_taxi_process_new_months_pipeline.py
```

The new-month DAG discovers raw periods at runtime and processes only months that are present in Object Storage but not yet fully loaded into ClickHouse.

## Data Pipeline

### Raw Layer

The raw layer contains the original NYC Yellow Taxi trip data in Parquet format.

### Bronze Layer

The bronze layer is created by:

```text
jobs/bronze_yellow_taxi.py
```

This layer stores ingested source data with minimal transformations and technical metadata.

Main additions:

- load timestamp;
- source system;
- source year;
- source month.

### Silver Layer

The silver layer is created by:

```text
jobs/silver_yellow_taxi.py
```

This layer contains cleaned and standardized taxi trip data.

Typical transformations include:

- calculating trip duration;
- extracting pickup date;
- extracting pickup hour;
- extracting pickup month;
- classifying trips into short, medium, and long;
- removing invalid records;
- writing bad records separately;
- writing a quality report.

The silver layer is partitioned by year and month.

### Gold Layer

The project follows a medallion-style data lake architecture:

```text
raw → bronze → silver → gold
```

The gold layer follows a Kimball-style analytical mart approach. Each gold table is designed around a clear analytical grain and business process:

- `gold_daily_trips` — daily taxi performance metrics;
- `gold_hourly_trips` — hourly demand and trip type analysis;
- `gold_location_pair_stats` — pickup/dropoff zone and route-level analytics;
- `gold_payment_type_stats` — payment behavior analytics.

This structure makes the final marts suitable for business reporting, ClickHouse analytics, and Superset dashboard visualizations.

Gold jobs:

```text
jobs/gold_daily_trips.py
jobs/gold_hourly_trips.py
jobs/gold_location_pair_stats.py
jobs/gold_payment_type_stats.py
```

Each gold mart is written to S3-compatible object storage and then loaded into ClickHouse.

## Data Quality Checks

The project includes data quality and validation jobs for different pipeline layers:

```text
jobs/check_yellow_taxi_quality.py
jobs/check_gold_schema.py
jobs/check_clickhouse_gold_quality.py
jobs/check_clickhouse_gold_month_quality.py
```

### Silver Data Quality Checks

The silver job validates records using checks such as:

- pickup datetime is not null;
- dropoff datetime is not null;
- dropoff datetime is greater than pickup datetime;
- trip distance is positive;
- fare amount is not negative;
- total amount is not negative;
- passenger count is valid;
- trip duration is positive and not unrealistically long;
- trip distance is not an extreme outlier;
- pickup date belongs to the expected processing month;
- `payment_type` is populated and belongs to the expected set of values;
- `PULocationID` and `DOLocationID` are populated and positive;
- derived `pickup_hour` is populated and belongs to the range from `0` to `23`.

These checks strengthen the silver layer contract for downstream gold marts. Payment, location, and pickup hour fields are required for payment analytics, route analytics, and hourly demand analytics.

For example, when processing January 2024, the allowed pickup date range is:

```text
2024-01-01 <= pickup_date < 2024-02-01
```

Invalid records are written to the bad records layer.

### Gold Object Storage Quality Checks

The gold Object Storage check validates that monthly gold marts were successfully written to S3-compatible Object Storage and meet expected quality rules before they are loaded into ClickHouse.

It checks that:

- all gold parquet marts can be read by Spark;
- all required columns are present;
- all gold marts are not empty;
- `pickup_date` values belong to the expected processing month;
- `trip_type` values are populated for hourly, route, and payment marts;
- `pickup_hour` values are valid for `gold_hourly_trips`;
- `payment_type_name` values are not empty for `gold_payment_type_stats`;
- `pickup_zone` and `dropoff_zone` values are not empty for `gold_location_pair_stats`.

This check helps catch issues in the gold parquet layer before data is loaded into the ClickHouse serving layer.

### ClickHouse Gold Quality Checks

The final ClickHouse quality check validates the analytical serving layer after all gold marts have been loaded into ClickHouse.

It checks that:

- all ClickHouse gold tables exist;
- all ClickHouse gold tables are not empty;
- `pickup_date` covers the expected full-year range from `2024-01-01` to `2024-12-31`;
- `trip_type` values are populated for hourly, route, and payment tables;
- `gold_location_pair_stats` has non-empty pickup and dropoff zone names;
- `gold_payment_type_stats` has non-empty payment type names.

This check acts as a final quality gate before the data is considered ready for BI reporting in Superset.

### ClickHouse Month-Level Quality Checks

The period refresh pipeline also includes a month-scoped ClickHouse quality check:

```text
jobs/check_clickhouse_gold_month_quality.py
```

This job validates the ClickHouse serving layer for one selected `year` and `month`.

It is used by the period refresh DAG after a selected month has been deleted from ClickHouse, reprocessed through Spark, and loaded back into the gold serving tables.

For a selected month, the check validates that:

- all expected ClickHouse gold tables exist;
- each gold table contains rows for the selected `year` and `month`;
- `pickup_date` values belong to the selected calendar month;
- `trips_count` values are positive;
- `trip_type` values are populated for hourly, route, and payment tables;
- `pickup_hour` values are valid for `gold_hourly_trips`;
- `gold_location_pair_stats` has non-empty pickup and dropoff zone names;
- `gold_payment_type_stats` has non-empty payment type names.

For example, for May 2024 the expected date range is:

```text
2024-05-01 <= pickup_date < 2024-06-01
```

This month-level check makes safe reloads possible without requiring a full-year ClickHouse validation after every monthly replacement.

## Gold Marts

The following gold marts are created and loaded into ClickHouse.

### `gold_daily_trips`

Daily trip metrics.

Main fields:

- `pickup_date`
- `trips_count`
- `total_revenue`
- `avg_check`
- `avg_trip_distance`
- `avg_trip_duration_minutes`
- `short_trips_count`
- `medium_trips_count`
- `long_trips_count`

Main use cases:

- daily trip volume;
- daily revenue;
- average check;
- average trip distance;
- average trip duration;
- trip type distribution.

### `gold_hourly_trips`

Hourly demand analytics.

Main fields:

- `pickup_date`
- `trip_type`
- `pickup_hour`
- `trips_count`
- `total_revenue`
- `avg_check`
- `avg_trip_distance`
- `avg_trip_duration_minutes`

Main use cases:

- trips by hour of day;
- demand distribution throughout the day;
- peak hour analysis;
- peak hour analysis by trip type;
- hourly revenue analysis.

### `gold_location_pair_stats`

Pickup and dropoff location pair statistics.

Main fields:

- `pickup_date`
- `trip_type`
- `pickup_location_id`
- `pickup_zone`
- `pickup_borough`
- `pickup_service_zone`
- `dropoff_location_id`
- `dropoff_zone`
- `dropoff_borough`
- `dropoff_service_zone`
- `trips_count`
- `total_revenue`
- `avg_check`
- `avg_trip_distance`
- `avg_trip_duration_minutes`

Main use cases:

- top routes by number of trips;
- top routes by revenue;
- route-level demand analysis;
- route-level demand analysis by trip type;
- pickup/dropoff zone analysis;
- ridesharing opportunity analysis for short nearby trips.

### `gold_payment_type_stats`

Payment type analytics.

Main fields:

- `pickup_date`
- `trip_type`
- `payment_type`
- `payment_type_name`
- `trips_count`
- `total_revenue`
- `avg_check`
- `total_tips`
- `avg_tip`
- `tips_share_from_revenue`

Main use cases:

- trips by payment type;
- revenue by payment type;
- average check by payment type;
- tips analysis;
- payment behavior analysis by trip type;
- payment preference trends over time.

The `trip_type` dimension (`short`, `medium`, `long`) is included in hourly, route, and payment marts to support analytical questions about trip behavior, peak demand by trip type, payment preferences, and ridesharing opportunities.

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

Shared ClickHouse HTTP execution logic is centralized in:

```text
jobs/clickhouse_utils.py
```

This module is reused by ClickHouse utility and quality-check jobs for query execution, Basic Auth handling, timeout handling, error handling, optional response printing, and JSON response parsing.

It is used by jobs such as:

```text
jobs/create_clickhouse_gold_tables.py
jobs/truncate_clickhouse_gold_tables.py
jobs/delete_clickhouse_gold_month.py
jobs/check_clickhouse_gold_quality.py
jobs/check_clickhouse_gold_month_quality.py
jobs/load_taxi_zone_centroids_to_clickhouse.py
jobs/run_clickhouse_sql_file.py
```

Before a full refresh, the DAG runs two ClickHouse preparation steps:

```text
jobs/create_clickhouse_gold_tables.py
jobs/truncate_clickhouse_gold_tables.py
```

The `create_clickhouse_gold_tables.py` job creates the ClickHouse database and gold tables if they do not already exist.

The `truncate_clickhouse_gold_tables.py` job clears existing gold table data before a full reload. This prevents duplicate data in ClickHouse when the full-year pipeline is rerun.

For safe month-level or interval refreshes, the project also includes:

```text
jobs/delete_clickhouse_gold_month.py
jobs/check_clickhouse_gold_month_quality.py
```

The `delete_clickhouse_gold_month.py` job deletes existing rows for a selected `year` and `month` from all ClickHouse gold tables before the month is reloaded.

This supports the `replace_period` refresh mode:

```text
delete selected month from ClickHouse
        │
        ▼
rebuild bronze, silver, and gold data for the month
        │
        ▼
load rebuilt gold marts to ClickHouse
        │
        ▼
validate the selected month in ClickHouse
```

This approach avoids truncating the entire serving layer when only one month or a small interval needs to be reprocessed.

In the full-year pipeline DAG, after all monthly gold marts are loaded into ClickHouse, the DAG runs a final full-year quality check:

```text
jobs/check_clickhouse_gold_quality.py
```

This job validates that the ClickHouse serving layer is ready for BI usage after the full-year reload:

- all gold tables exist;
- all gold tables are not empty;
- `pickup_date` covers the expected full-year range from `2024-01-01` to `2024-12-31`;
- `trip_type` values are populated in hourly, route, and payment tables;
- enriched pickup and dropoff location names are populated;
- payment type names are populated.

For period refresh runs, ClickHouse validation is performed by the month-level quality check after each selected month is reloaded.

If any of these checks fail, the Airflow DAG fails as well.

ClickHouse is used as an analytical serving layer for fast BI queries from Superset.

## Final ClickHouse Output

After processing the full year 2024, the ClickHouse gold tables contain the following date range:

```text
2024-01-01 → 2024-12-31
```

Final row counts after the full-year pipeline run with the `trip_type` dimension:

```text
gold_daily_trips              366
gold_hourly_trips             26349
gold_payment_type_stats       5490
gold_location_pair_stats      3460697
```

The hourly, payment, and route marts include the `trip_type` dimension:

```text
short
medium
long
```

The `gold_location_pair_stats` mart is enriched with readable taxi zone names, for example:

```text
237 → Upper East Side South
236 → Upper East Side North
```

## Airflow Orchestration

The pipeline is orchestrated with Apache Airflow.

The project includes three Airflow DAGs for different processing scenarios:

```text
dags/nyc_taxi_pipeline.py
dags/nyc_taxi_period_refresh_pipeline.py
dags/nyc_taxi_process_new_months_pipeline.py
```
### Local Spark Resource Control with Airflow Pools

The project uses an Airflow Pool named `spark_pool` to limit concurrent local Spark execution.

All Spark-heavy Airflow tasks that run `spark-submit` are assigned to this pool, including:

- Bronze monthly jobs;
- Silver monthly jobs;
- Silver quality checks;
- Gold mart jobs;
- Gold Object Storage schema and quality checks;
- Spark-based ClickHouse load jobs.

In the local Docker environment, the recommended pool configuration is:

```text
Pool name: spark_pool
Slots: 1
Description: Limits concurrent local Spark jobs
```

This prevents Spark-heavy tasks from different DAGs from running at the same time and overloading the local Docker/Spark environment.

The pool complements DAG-level safety settings such as:

```text
max_active_runs = 1
max_active_tasks = 1
```

`max_active_runs` and `max_active_tasks` keep individual DAG runs sequential, while `spark_pool` provides cross-DAG Spark resource control.

### Airflow Task Hardening and Failure Callbacks

The project also includes a basic Airflow task hardening layer for local production-like reliability.

All NYC Taxi DAGs use centralized Airflow runtime settings for:

- retry delay;
- Spark task execution timeout;
- lightweight Python/ClickHouse task execution timeout;
- shared task failure callback.

Default runtime settings are defined in `jobs/config.py`:

```text
AIRFLOW_RETRY_DELAY_MINUTES = 5
SPARK_TASK_EXECUTION_TIMEOUT_MINUTES = 30
PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES = 10
```

These values can be overridden through environment variables if needed.

Spark-heavy tasks use the Spark execution timeout. This includes Bronze, Silver, Silver quality, Gold mart, Gold Object Storage schema/quality, and Spark-based ClickHouse load tasks.

Lightweight Python/ClickHouse service tasks use the Python execution timeout. This includes ClickHouse table creation, truncation, month deletion, runtime config reading, raw period discovery, logging tasks, and ClickHouse quality checks.

Each DAG also uses a shared Airflow failure callback:
```
jobs/airflow_callbacks.py
```
The callback builds a structured failure message with the DAG ID, task ID, run ID, try number, logical date, log URL, and exception details.

At the current stage, the callback writes this message to Airflow task logs. This creates a reusable foundation for future Telegram, Slack, email, or webhook-based alerting.

### Full-Year Pipeline DAG

The main full-year pipeline DAG is:

```text
nyc_taxi_pipeline
```

This DAG processes all configured 2024 months and runs the full pipeline:

```text
create ClickHouse gold tables
        │
        ▼
truncate ClickHouse gold tables
        │
        ▼
bronze monthly jobs
        │
        ▼
silver monthly jobs
        │
        ▼
silver quality checks
        │
        ▼
gold marts
        │
        ▼
gold Object Storage quality checks
        │
        ▼
load gold marts to ClickHouse
        │
        ▼
check ClickHouse gold quality
```

The DAG first ensures that all ClickHouse gold tables exist, then truncates them before running the full-year reload.

For each month, the DAG validates gold parquet marts in Object Storage before loading them into ClickHouse.

The final `check_clickhouse_gold_quality` task verifies that the ClickHouse serving layer is complete and ready for Superset reporting.

This DAG is intended for controlled full-year rebuilds, not for incremental monthly updates.

### Period Refresh DAG

The project also includes a period-based refresh DAG:

```text
nyc_taxi_period_refresh_pipeline
```

This DAG is designed for safe month-level and interval reloads.

Current supported refresh mode:

```text
replace_period
```

In `replace_period` mode, the DAG processes a selected interval of months. For each selected month, it:

```text
delete selected month from ClickHouse
        │
        ▼
run bronze monthly job
        │
        ▼
run silver monthly job
        │
        ▼
run silver quality check
        │
        ▼
build gold marts
        │
        ▼
run gold Object Storage schema and quality check
        │
        ▼
load gold marts to ClickHouse
        │
        ▼
run ClickHouse month-level quality check
```

The period refresh DAG uses Airflow runtime configuration instead of static period constants inside the DAG file.

A one-month reload can be triggered with Airflow Trigger DAG config:

```json
{
  "start_year": "2024",
  "start_month": "05",
  "end_year": "2024",
  "end_month": "05",
  "refresh_mode": "replace_period"
}
```

This reloads only May 2024 without truncating all ClickHouse gold tables.

A multi-month interval refresh can be triggered with:

```json
{
  "start_year": "2024",
  "start_month": "01",
  "end_year": "2024",
  "end_month": "02",
  "refresh_mode": "replace_period"
}
```

This sequentially reloads January and February 2024.

The DAG reads the runtime config in:

```text
read_period_refresh_config
```

Then it generates selected monthly periods and processes them with Airflow dynamic task mapping:

```text
process_month[0]
process_month[1]
...
```

Each mapped `process_month` group runs the full monthly replacement pipeline for one selected period.

Default DAG params are intentionally safe:

```text
start_year = 2024
start_month = 01
end_year = 2024
end_month = 01
refresh_mode = replace_period
```

This prevents an accidental full-year reload when the DAG is triggered without explicit runtime config.

The period refresh config is validated by:

```text
jobs/period_refresh_config.py
```

The helper validates:

- supported refresh mode;
- start and end year/month values;
- chronological period order;
- maximum allowed refresh interval;
- generated period parameters for mapped monthly processing.

The period refresh DAG has been manually tested in Airflow with runtime config for one-month and interval reload scenarios.

Current implementation keeps Spark execution local through the Airflow container for the local Docker setup.

### New-Month Processing DAG

The project also includes a new-month processing DAG:

```text
nyc_taxi_process_new_months_pipeline
```

This DAG is designed to automatically process newly arrived raw monthly files.

It uses:

```text
jobs/raw_discovery.py
```

to compare raw monthly Yellow Taxi files in Object Storage with periods already fully processed in ClickHouse.

Current discovery logic:

```text
new periods = raw periods from Object Storage - fully processed periods from ClickHouse
```

A period is considered fully processed only if it exists in all expected ClickHouse gold tables:

```text
gold_daily_trips
gold_hourly_trips
gold_location_pair_stats
gold_payment_type_stats
```

This protects the pipeline from skipping partially loaded months.

Processing flow:

```text
discover new raw periods
        │
        ▼
process each discovered month
        │
        ▼
load the month into ClickHouse
        │
        ▼
run month-level quality checks
```

The DAG uses Airflow dynamic task mapping with a mapped monthly TaskGroup:

```text
process_month[0]
process_month[1]
...
```

Each mapped `process_month` group runs the full monthly pipeline for one discovered period:

```text
delete ClickHouse month
        │
        ▼
bronze monthly job
        │
        ▼
silver monthly job
        │
        ▼
silver quality check
        │
        ▼
gold_daily / gold_hourly / gold_payment / gold_location
        │
        ▼
gold schema check
        │
        ▼
ClickHouse loads
        │
        ▼
ClickHouse month quality check
```

Safety behavior:

- only raw periods not fully processed in ClickHouse are selected;
- before processing a discovered month, the DAG deletes this month from ClickHouse gold tables;
- for a truly new month, this delete step removes zero rows;
- for a partially loaded month, it cleans up inconsistent ClickHouse data before rebuilding;
- if no new raw periods are found, the DAG finishes successfully as a no-op.

Local execution settings:

```text
max_active_runs = 1
max_active_tasks = 1
```

These settings keep Spark jobs sequential and safe for local Docker execution.

Spark-heavy tasks in this DAG also use the shared Airflow `spark_pool`, which prevents Spark jobs from this DAG and other NYC Taxi DAGs from running at the same time in the local environment.

The new-month DAG was validated on real TLC Yellow Taxi data for:

```text
2025-01
```

Validation result:

- `2025-01` was detected as a new raw period;
- the DAG processed the month end-to-end;
- all ClickHouse gold marts received rows for `2025-01`;
- month-level ClickHouse quality checks passed;
- a second DAG run detected no new periods and completed successfully as a no-op.

Airflow is used to manage task dependencies, retries, execution visibility, and safe orchestration across all pipeline modes.

## Superset BI Dashboard

Apache Superset is used as the BI and analytical visualization layer.

The dashboard is built on top of ClickHouse gold marts and Superset virtual datasets. It provides an executive-level overview of NYC Yellow Taxi performance, demand patterns, payment behavior, geospatial demand concentration, and grouped ride opportunities.

Dashboard name: `NYC Taxi BI Dashboard`.

The exported dashboard PDF is available here: [`docs/nyc_taxi_bi_dashboard.pdf`](docs/nyc_taxi_bi_dashboard.pdf).

This PDF provides a static portfolio-friendly version of the final Superset dashboard.

### Superset Virtual Datasets

Some dashboard charts are based directly on ClickHouse gold marts, while others use Superset virtual datasets.

Virtual datasets are used to prepare chart-specific analytical views without creating additional physical tables in ClickHouse. They include:

- pickup and dropoff zone map datasets with taxi zone centroid coordinates;
- payment preference trend dataset;
- grouped ride opportunity dataset;
- route-level and trip-type analytical views.

For geospatial charts, virtual datasets deduplicate taxi zone centroids to one row per `location_id` before joining them with pickup and dropoff demand. This prevents duplicated metrics for multi-part taxi zones.

The dashboard includes the following analytical sections.

### Executive Overview

KPI cards summarize full-year 2024 performance:

- total trips;
- total revenue;
- average check;
- average cost per mile;
- average trip distance;
- average trip duration.

### Daily Performance

Daily trend charts show:

- daily trips;
- daily revenue.

These charts help identify seasonality, weekly demand patterns, demand drops, and revenue fluctuations.

### Trip Type Analytics

Trip type analytics are based on the `trip_type` dimension:

- `short`;
- `medium`;
- `long`.

The dashboard includes:

- trip distribution by type;
- revenue by trip type;
- average cost per mile by trip type.

This section helps compare trip volume, revenue contribution, and passenger cost efficiency across different distance-based trip segments.

### Peak Demand

Peak demand charts show:

- trips by pickup hour;
- peak hours by trip type.

The heatmap highlights how short, medium, and long trips have different demand patterns throughout the day.

### Demand Geography

Geography-focused charts show:

- top pickup zones by trips;
- top dropoff zones by trips;
- top routes by trips;
- top routes by revenue.

These charts help identify high-demand zones, high-value routes, airport-driven demand, and dense Manhattan pickup/dropoff patterns.

### Geospatial Demand

The dashboard includes map-based visualizations:

- pickup demand map;
- dropoff demand map.

These maps use taxi zone centroid coordinates joined with aggregated pickup and dropoff demand. They help visually identify where taxi demand is geographically concentrated.

### Payment Analytics

Payment analytics include:

- payment preference trend over time;
- payment methods by trip type;
- average check by payment type.

These charts show that card payments dominate across trip types and help monitor changes in payment behavior over time.

### Ridesharing Opportunities

The dashboard includes a dedicated analytical section for grouped ride opportunities:

- top short-trip ridesharing candidate routes;
- vehicle revenue uplift for top short-trip candidates.

This section focuses on high-volume short trips between nearby zones and evaluates grouped ride potential using a simplified business model.

The dashboard includes a date filter based on `pickup_date`, so charts can be filtered by reporting period.

## Geospatial Enrichment

To support map visualizations in Superset, the project includes a taxi zone centroid enrichment step.

The original gold marts contain taxi zone identifiers and readable zone names, but they do not contain geographical coordinates.

To enable pickup and dropoff demand maps, taxi zone geometry was processed into a centroid lookup table:

```text
data/geo/taxi_zone_centroids.csv
```

This file contains:

- `location_id`;
- `borough`;
- `zone`;
- `longitude`;
- `latitude`.

The lookup table is loaded into ClickHouse using:

```text
jobs/load_taxi_zone_centroids_to_clickhouse.py
```

Target ClickHouse table:

```text
nyc_taxi.taxi_zone_centroids
```

Some taxi zones are represented by multiple geometry parts. For example:

- `56` — Corona;
- `103` — Governor's Island / Ellis Island / Liberty Island.

To prevent metric duplication during joins, Superset virtual datasets deduplicate centroids to one row per `location_id` before joining them with pickup and dropoff demand.

## Analytical Questions and Business Recommendations

The project includes a dedicated analytical layer for answering business questions from the original project requirements.

Analytical SQL queries are stored in:

```text
sql/analytics/
```

The analytical summary document is stored in:

```text
docs/analytics_summary.md
```

The analysis covers:

- zones with the highest pickup and dropoff demand;
- peak taxi demand hours;
- trip distribution by `trip_type`;
- peak hours for short, medium, and long trips;
- top pickup and dropoff zones by trip type;
- payment methods by trip type;
- payment preference evolution over time;
- ridesharing opportunities for short nearby trips;
- a simplified economic impact model for grouped short trips.

The analytical summary also includes business recommendations related to zone-based pricing, peak-hour pricing, short-trip promotions, card payment reliability, and grouped rides.

## Automated Tests and CI

The project includes automated tests for configuration helpers, Airflow DAG structure, Spark transformation logic, and data quality validation gates.

Test files:

```text
tests/test_config.py
tests/test_airflow_callbacks.py
tests/test_period_utils.py
tests/test_period_refresh_config.py
tests/test_clickhouse_utils.py
tests/test_raw_discovery.py
tests/test_dag.py
tests/test_period_refresh_dag.py
tests/test_process_new_months_dag.py
tests/test_silver_transformations.py
tests/test_gold_daily_transformations.py
tests/test_gold_hourly_transformations.py
tests/test_gold_payment_type_transformations.py
tests/test_gold_location_pair_transformations.py
tests/test_check_yellow_taxi_quality.py
tests/test_check_gold_schema.py
tests/test_check_clickhouse_gold_quality.py
tests/test_delete_clickhouse_gold_month.py
tests/test_check_clickhouse_gold_month_quality.py
```

### `test_config.py`

This test module validates project configuration logic:

- monthly date boundary calculation;
- S3/Object Storage path helpers;
- raw, bronze, silver, quality, bad records, and gold layer paths;
- taxi zone lookup path;
- Airflow retry delay default setting;
- Spark task execution timeout default setting;
- Python/lightweight task execution timeout default setting.

### `test_airflow_callbacks.py`

This test module validates shared Airflow failure callback logic:

- safe value extraction from Airflow callback context dictionaries;
- safe attribute extraction from Airflow objects;
- structured failure alert message generation;
- fallback behavior when optional Airflow context fields are missing;
- callback output to task logs.

### `test_clickhouse_utils.py`

This test module validates shared ClickHouse HTTP helper logic:

- ClickHouse HTTP URL construction;
- POST request creation;
- Basic Auth handling;
- optional response printing;
- HTTP and connection error handling;
- JSON response parsing;
- single-row JSON result validation.

### `test_raw_discovery.py`

This test module validates raw monthly file discovery logic without requiring real Object Storage or ClickHouse access:

- raw Yellow Taxi object key parsing;
- invalid raw key filtering;
- wrong taxi type and wrong layer filtering;
- mismatched path and filename period rejection;
- duplicate raw period handling;
- chronological period sorting;
- raw minus fully processed period comparison;
- processed period parsing from mocked ClickHouse JSON results;
- fully processed period detection using intersection across all ClickHouse gold tables;
- partially loaded period exclusion;
- formatting discovered periods for logs.

### `test_dag.py`

This test module validates Airflow orchestration logic:

- DAG imports without errors;
- `nyc_taxi_pipeline` DAG exists;
- ClickHouse preparation and quality tasks exist;
- monthly tasks for January and December exist;
- total task count is correct;
- `create_clickhouse_gold_tables` runs before `truncate_clickhouse_gold_tables`;
- the first bronze task runs after ClickHouse preparation;
- monthly processing order is preserved;
- gold tasks run before ClickHouse load tasks;
- gold Object Storage quality check runs after all monthly gold marts and before ClickHouse load tasks;
- Spark-heavy tasks use the shared Airflow `spark_pool`;
- lightweight ClickHouse service tasks do not use the Spark pool;
- final ClickHouse gold quality check runs after all December load tasks;
- all tasks use the shared Airflow failure callback;
- all tasks use the configured retry delay;
- Spark-heavy tasks use the configured Spark execution timeout;
- lightweight ClickHouse service tasks use the configured Python execution timeout.

### `test_period_refresh_dag.py`

This test module validates the period refresh Airflow DAG structure:

- DAG imports without errors;
- `nyc_taxi_period_refresh_pipeline` DAG exists;
- local-safe concurrency settings are configured;
- runtime DAG params are configured with safe defaults;
- top-level create-tables, config-read, logging, processing, and finish tasks exist;
- mapped `process_month` TaskGroup tasks exist;
- full-rebuild-only tasks such as `truncate_clickhouse_gold_tables` and `check_clickhouse_gold_quality` are not used in `replace_period` mode;
- old static month-specific task IDs are not used anymore;
- total task count is correct for the runtime-mapped DAG structure;
- ClickHouse table creation runs before runtime config reading;
- config reading runs before logging and monthly processing;
- monthly processing dependencies inside the mapped TaskGroup are correct;
- Gold marts run after the Silver quality check;
- Gold schema check runs after all monthly Gold marts;
- ClickHouse load tasks run after the Gold schema check;
- ClickHouse month-level quality check runs after all ClickHouse loads;
- `finish` uses a no-op-safe trigger rule and runs after logging and monthly processing;
- Spark-heavy mapped TaskGroup tasks use the shared Airflow `spark_pool`;
- non-Spark tasks remain outside the Spark pool;
- all tasks use the shared Airflow failure callback;
- all tasks use the configured retry delay;
- Spark-heavy mapped TaskGroup tasks use the configured Spark execution timeout;
- non-Spark processing tasks use the configured Python execution timeout.

### `test_process_new_months_dag.py`

This test module validates the new-month processing Airflow DAG structure:

- DAG imports without errors;
- `nyc_taxi_process_new_months_pipeline` DAG exists;
- local-safe concurrency settings are configured;
- top-level discovery, logging, processing, and finish tasks exist;
- mapped `process_month` TaskGroup tasks exist;
- full-rebuild-only tasks such as `truncate_clickhouse_gold_tables` are not used;
- ClickHouse table creation runs before raw discovery;
- discovery runs before logging and monthly processing;
- monthly processing dependencies inside the mapped TaskGroup are correct;
- Gold marts run after the Silver quality check;
- Gold schema check runs after all monthly Gold marts;
- ClickHouse load tasks run after the Gold schema check;
- ClickHouse month-level quality check runs after all ClickHouse loads;
- `finish` uses a no-op-safe trigger rule and runs after logging and monthly processing;
- Spark-heavy mapped TaskGroup tasks use the shared Airflow `spark_pool`;
- discovery, logging, ClickHouse cleanup, month-level quality, and finish tasks remain outside the Spark pool;
- all tasks use the shared Airflow failure callback;
- all tasks use the configured retry delay;
- Spark-heavy mapped TaskGroup tasks use the configured Spark execution timeout;
- discovery, logging, ClickHouse cleanup, and month-level quality tasks use the configured Python execution timeout.

### `test_period_utils.py`

This test module validates period helper logic:

- generation of a one-month period;
- generation of a same-year month interval;
- generation of a cross-year month interval;
- normalization of integer and string year/month values;
- validation of invalid months;
- validation that start period is not later than end period.

### `test_period_refresh_config.py`

This test module validates runtime configuration logic for the period refresh DAG:

- reading config values with safe defaults;
- handling missing, empty, and integer config values;
- validation of supported refresh modes;
- rejection of unsupported refresh modes;
- normalization of selected year/month values;
- generation of one-month refresh periods;
- generation of same-year and cross-year refresh intervals;
- validation of invalid months;
- validation that start period is not later than end period;
- safety limit for maximum refresh interval size;
- formatting selected periods for logs.

### `test_delete_clickhouse_gold_month.py`

This test module validates ClickHouse month deletion logic without requiring a real ClickHouse connection:

- year/month normalization;
- ClickHouse `ALTER TABLE ... DELETE` query construction;
- month-level `count()` query construction;
- parsing of ClickHouse count responses;
- polling logic for asynchronous ClickHouse mutations;
- execution flow across all configured gold tables.

### `test_check_clickhouse_gold_month_quality.py`

This test module validates month-level ClickHouse quality-check logic without requiring a real ClickHouse connection:

- table existence query construction;
- month-scoped quality query construction;
- JSON response parsing;
- common month metrics validation;
- `trip_type` validation;
- hourly pickup hour validation;
- pickup/dropoff zone validation;
- payment type name validation;
- execution flow across all configured gold tables.

### Spark Transformation Tests

The transformation tests validate business logic using small in-memory Spark DataFrames.

Covered transformation modules:

- `silver_yellow_taxi.py`;
- `gold_daily_trips.py`;
- `gold_hourly_trips.py`;
- `gold_payment_type_stats.py`;
- `gold_location_pair_stats.py`.

The tests validate:

- Silver DQ flags;
- bad record condition logic;
- Silver analytical columns such as `pickup_date`, `pickup_hour`, `pickup_month`, and `trip_type`;
- daily Gold mart aggregations;
- hourly Gold mart aggregations;
- payment type mapping and payment metrics;
- pickup/dropoff route-level aggregations;
- taxi zone lookup enrichment.

### Quality Gate Tests

The quality gate tests validate driver-side and Spark-expression validation logic without requiring real S3 or ClickHouse access.

Covered quality modules:

- `check_yellow_taxi_quality.py`;
- `check_gold_schema.py`;
- `check_clickhouse_gold_quality.py`;
- `check_clickhouse_gold_month_quality.py`;
- `delete_clickhouse_gold_month.py`;
- `clickhouse_utils.py`;
- `raw_discovery.py`;
- `period_refresh_config.py`.

The tests validate:

- Silver row count and row-loss checks;
- required Silver fields not being NULL;
- invalid payment types, location IDs, pickup hours, and distances;
- Gold Object Storage schema and quality checks;
- ClickHouse final serving-layer validation logic;
- SQL query construction for ClickHouse quality checks;
- JSON result parsing and validation for ClickHouse quality metrics;
- ClickHouse month-level delete query construction;
- ClickHouse month-level quality validation logic;
- shared ClickHouse HTTP helper behavior, including authentication, response parsing, and error handling;
- raw monthly file discovery logic for identifying newly arrived data;
- month-scoped serving-layer validation for safe period refreshes;
- runtime period refresh config parsing, validation, default values, interval generation, and safety limits.

### Running Tests

Run tests locally:

```bash
PYTHONPATH=jobs python -m pytest tests -v
```

Run tests inside the Airflow container:

```bash
docker exec -it nyc_taxi_airflow bash -lc '
cd /opt/airflow &&
PYTHONPATH=/opt/airflow/jobs python -m pytest tests -v
'
```

### GitHub Actions CI

The project uses GitHub Actions to run automated checks on push and pull requests.

Workflow file:

```text
.github/workflows/ci.yml
```

The CI pipeline runs:

- Python syntax check for DAG and job files;
- automated tests with `pytest`;
- Airflow DAG structure tests;
- Spark transformation unit tests;
- Silver, Gold, and ClickHouse quality-gate unit tests.

CI command:

```bash
PYTHONPATH=jobs python -m pytest tests -v
```

This helps ensure that changes do not break configuration helpers, Airflow DAG imports, task dependencies, transformation logic, or quality validation gates before they are merged into `main`.


## Pipeline Optimization

The project includes an optimization pass across Spark, Object Storage, ClickHouse loading jobs, and quality checks.

The main goal was to reduce redundant Spark actions, avoid repeated reads from Object Storage, make quality checks more efficient, and improve production-like reliability without changing the business logic of the pipeline.

Main improvements:

- removed non-critical `count()` actions used only for logging before write operations;
- removed `show()` preview actions from production-like Spark jobs;
- removed unnecessary `orderBy()` operations before writing Parquet datasets, because row order is not a reliable storage contract for Parquet and should be handled in SQL or BI queries;
- consolidated multiple data quality checks into single aggregate operations where possible;
- reduced repeated reads from Object Storage in Silver, Gold, and quality-check jobs;
- kept `dq_df` persisted with `StorageLevel.DISK_ONLY` in the Silver job to reduce recomputation without increasing Java heap pressure;
- avoided caching large intermediate DataFrames such as `silver_df` to reduce Java heap out-of-memory risks;
- replaced full parquet `count()` checks in ClickHouse load jobs with lightweight `take(1)` non-empty checks;
- replaced full ClickHouse table counts after monthly loads with targeted counts for the loaded `year` and `month`;
- consolidated final ClickHouse quality checks into fewer aggregate SQL queries;
- centralized shared ClickHouse HTTP helper logic in `clickhouse_utils.py` and reused it across ClickHouse utility and quality-check jobs;
- added `try/finally` blocks to Spark jobs to ensure `spark.stop()` is called even if a job fails;
- improved the taxi zone centroid loader with stronger validation checks, a working-directory-independent CSV path, and explicit handling of known duplicate `location_id` values caused by multi-part taxi zone geometries.

As a result, the pipeline now performs fewer redundant actions, avoids several repeated full scans, and provides more reliable validation while keeping the data processing logic unchanged.

During local benchmarking, the strongest runtime improvements were observed in quality-check jobs where multiple repeated `count()` actions were consolidated into single aggregate checks. Smaller but consistent improvements were also observed in Gold mart and ClickHouse load jobs after removing non-critical actions and unnecessary sorting before Parquet writes.

During a full-year local Airflow DAG run, total pipeline runtime decreased from approximately 2 hours 2 minutes before the optimization pass to approximately 1 hour 39 minutes after the optimization pass. This reduced the full DAG runtime by about 23 minutes, or approximately 18.9%.

## Monitoring and Alerting

The production-like monitoring and alerting strategy is documented in:

```text
docs/monitoring_plan.md
```
The plan includes Airflow DAG monitoring, Spark task runtime SLA thresholds, Silver and Gold data quality checks, ClickHouse serving-layer validation, Superset dashboard monitoring, and incident response guidelines.

Part of the monitoring strategy is already implemented in the Airflow layer:

- all DAGs use explicit retry delay;
- Spark-heavy tasks have execution timeouts to prevent long-running or hanging Spark jobs;
- lightweight Python/ClickHouse tasks have execution timeouts;
- all DAGs use a shared failure callback;
- failure callback messages are written to Airflow task logs in a structured format.

The current failure callback is intentionally transport-agnostic. External notifications such as Telegram or Slack can be added later on top of the same callback message builder.

## How to Run Locally

### 1. Configure Environment Variables

Create a local `.env` file using `.env.example` as a template.

Required configuration includes:

- object storage bucket;
- S3-compatible endpoint;
- object storage credentials;
- ClickHouse credentials;
- Airflow admin credentials;
- Superset admin credentials;
- Superset secret key;
- Airflow webserver secret key.

### 2. Start Infrastructure

```bash
docker compose up -d --build
```

This starts:

- PostgreSQL for Airflow metadata;
- Airflow webserver and scheduler;
- ClickHouse;
- Superset.

### 3. Open Airflow

```text
http://localhost:8080
```

Run the full-year pipeline DAG:

```text
nyc_taxi_pipeline
```

For safe month-level or interval refreshes, use the period refresh DAG:

```text
nyc_taxi_period_refresh_pipeline
```

The period refresh DAG accepts runtime configuration through Airflow Params / Trigger DAG config.

Example one-month reload:

```json
{
  "start_year": "2024",
  "start_month": "05",
  "end_year": "2024",
  "end_month": "05",
  "refresh_mode": "replace_period"
}
```

Example two-month interval refresh:

```json
{
  "start_year": "2024",
  "start_month": "01",
  "end_year": "2024",
  "end_month": "02",
  "refresh_mode": "replace_period"
}
```

Default DAG params are intentionally safe and reload only one month if no explicit runtime config is provided:

```text
start_year = 2024
start_month = 01
end_year = 2024
end_month = 01
refresh_mode = replace_period
```

The period refresh DAG should be used carefully because it deletes selected month data from ClickHouse before reloading it.

For automated processing of newly arrived raw monthly files, use the new-month processing DAG:

```text
nyc_taxi_process_new_months_pipeline
```

This DAG discovers raw Yellow Taxi monthly files in Object Storage and processes only periods that are not yet fully loaded into all ClickHouse gold tables.

The new-month processing DAG also deletes a discovered month from ClickHouse before processing it. For a truly new month this removes zero rows. For a partially loaded month, this cleans up inconsistent ClickHouse data before rebuilding and reloading the month.

If no new raw periods are found, the DAG completes successfully as a no-op.

### 4. Open ClickHouse

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

### 5. Open Superset

```text
http://localhost:8088
```

Open dashboard:

```text
NYC Taxi BI Dashboard
```

If table schemas change, refresh Superset datasets:

```text
Data → Datasets → Edit dataset → Columns → Sync columns from source → Save
```

## Screenshots

### Superset Dashboard — Executive Overview

![Superset Executive Overview](screenshots/superset_01_executive_overview.png)

### Superset Dashboard — Trip Type and Peak Demand

![Superset Trip Type and Peak Demand](screenshots/superset_02_trip_type_and_peak_demand.png)

### Superset Dashboard — Heatmap and Geospatial Maps

![Superset Heatmap and Geospatial Maps](screenshots/superset_03_heatmap_and_geo_maps.png)

### Superset Dashboard — Geospatial Demand and Top Zones

![Superset Geospatial Demand and Top Zones](screenshots/superset_04_geo_demand_zones.png)

### Superset Dashboard — Routes and Payment Trends

![Superset Routes and Payment Trends](screenshots/superset_05_routes_and_payment_trends.png)

### Superset Dashboard — Payment Analytics

![Superset Payment Analytics](screenshots/superset_06_payment_analytics.png)

### Superset Dashboard — Ridesharing Opportunities

![Superset Ridesharing Opportunities](screenshots/superset_07_ridesharing_opportunities.png)

### Airflow Successful Pipeline Run

![Airflow Successful Pipeline Run](screenshots/airflow_successful_run_graph.png)

### ClickHouse Gold Tables

![ClickHouse Gold Tables](screenshots/clickhouse_gold_tables.png)

## Future Improvements

Possible future improvements:

- add a protected `full_rebuild` mode to the period refresh DAG or keep it as a separate controlled full-year DAG;
- add ClickHouse schema migration/versioning using the shared `clickhouse_utils.py` module as the common execution layer;
- add dbt layer for analytical transformations or document dbt as a future analytical modeling layer;
- verify Superset dashboard import on a clean Superset instance as a reproducible BI artifact;
- migrate Spark compute to Yandex Data Proc or another cloud Spark runtime as a separate cloud execution phase;
- replace local subprocess-based Spark execution in Airflow DAGs with production-like job submission to Yandex Data Proc or another managed Spark compute layer, where Airflow only orchestrates, monitors job status, and fails tasks when cloud jobs fail;
- decide whether ClickHouse should remain local for portfolio usage or move to a cloud-hosted serving layer;
- add external notifications, such as Telegram or Slack alerts, on top of the existing Airflow failure callback;
- add task-family-specific Airflow execution timeouts based on the SLA thresholds documented in `docs/monitoring_plan.md`;
- add runtime trend monitoring for Airflow tasks and Spark jobs;
- add Prometheus, Grafana, and Alertmanager for infrastructure and pipeline observability if the project is deployed as a longer-running environment;
- add taxi zone polygon-based choropleth maps instead of centroid-based point maps;
- add a final project walkthrough document for portfolio and interview preparation.