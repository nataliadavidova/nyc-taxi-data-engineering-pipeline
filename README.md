# NYC Taxi Data Engineering Pipeline

End-to-end data engineering project for processing historical NYC Yellow Taxi trip data using a medallion architecture, Apache Spark ETL jobs, Airflow orchestration, ClickHouse as an analytical serving layer, and Apache Superset for BI dashboards.

The current raw dataset contains 120 monthly Yellow Taxi files covering:

```text
2016-01 → 2025-12
```

The project supports three Airflow processing scenarios:

- protected full rebuild of all validated raw periods;
- safe month-level and interval refreshes;
- automated processing of newly discovered raw months.

## Project Overview

The project implements a production-like batch data platform for historical NYC Yellow Taxi data.

Monthly Parquet files are read from S3-compatible Object Storage and processed through raw, bronze, silver, and gold layers. The resulting analytical marts are validated, loaded into ClickHouse, and exposed through Apache Superset.

The platform supports three Airflow processing modes:

- protected full rebuild of all validated raw periods;
- safe replacement of one month or a limited interval;
- automated processing of newly discovered or partially loaded months.

The implementation combines PySpark ETL, Airflow dynamic task mapping, multi-layer data quality gates, ClickHouse serving, Docker-based infrastructure, automated testing, CI, and failure alerting.



## Project Highlights

- Built an end-to-end batch data engineering pipeline for 120 monthly NYC Yellow Taxi periods covering `2016-01 → 2025-12`.
- Implemented a medallion architecture with raw, bronze, silver, and gold layers using PySpark and Parquet.
- Created four analytical marts for daily performance, hourly demand, payment behavior, and pickup/dropoff route analysis.
- Loaded business-ready marts into ClickHouse and built an Apache Superset dashboard for operational, financial, and geospatial analytics.
- Implemented three Airflow processing scenarios:
  - protected full rebuild of all validated raw periods;
  - safe month-level and interval replacement;
  - automated processing of newly discovered raw months.
- Replaced the original static 2024 pipeline with a dynamically mapped full rebuild DAG that discovers monthly raw periods at runtime.
- Protected destructive full rebuilds with explicit operator confirmations, expected-range validation, missing-period detection, and unexpected-period detection before ClickHouse truncation.
- Validated both pre-truncation safety gates with real Airflow negative runs; ClickHouse row counts remained unchanged when confirmation or raw-range validation failed.
- Added Silver, Gold Object Storage, and ClickHouse month-level quality gates to prevent invalid or incomplete data from reaching the BI layer.
- Added production-like reliability controls with retries, task-specific execution timeouts, a shared Spark pool, structured failure callbacks, and optional Telegram alerts.
- Added more than 300 automated tests and GitHub Actions CI covering transformations, DAG structure, dynamic mapping, raw discovery, ClickHouse utilities, and destructive-operation safety.
- Optimized Spark and quality-check workloads, reducing the historical 2024 full-pipeline benchmark from approximately 2 hours 2 minutes to 1 hour 39 minutes.


## Business Value

The platform provides a reusable historical analytics foundation for NYC Yellow Taxi operations.

ClickHouse marts and the Superset dashboard support:

- demand and revenue monitoring over time;
- peak-hour, trip-type, route, and zone analysis;
- payment behavior and tip analytics;
- operational and executive reporting;
- identification of high-volume routes and grouped-ride opportunities;
- period-over-period comparison and anomaly investigation.

The separation between Object Storage, Spark transformation layers, ClickHouse serving tables, and Superset allows processing, validation, and reporting logic to evolve independently.

Detailed analytical findings and business recommendations are documented in:

[`docs/analytics_summary.md`](docs/analytics_summary.md)


## Tech Stack and Architecture

| Component | Role |
|---|---|
| Python / PySpark | ETL jobs, validation, discovery, and utility scripts |
| Apache Spark | distributed batch processing |
| Apache Airflow | orchestration, retries, timeouts, dynamic task mapping, and failure handling |
| S3-compatible Object Storage | raw, Bronze, Silver, Gold, bad-record, and quality datasets |
| Parquet | columnar storage format for lake layers |
| ClickHouse | analytical serving layer |
| Apache Superset | dashboards and exploratory analytics |
| Docker Compose | reproducible local infrastructure |
| PyTest | automated unit and pipeline-structure tests |
| GitHub Actions | continuous integration |
| SQL | ClickHouse validation and analytical queries |

High-level data flow:

```text
S3-compatible Object Storage
        │
        ▼
Raw → Bronze → Silver → Gold
        │                    │
        │                    ▼
        │              ClickHouse
        │                    │
        └────────────────────▼
                     Apache Superset
```

Pipeline orchestration:

```text
Apache Airflow
→ selects processing periods
→ runs Spark jobs
→ applies quality gates
→ loads ClickHouse
→ handles retries and failures
```

Detailed architecture is documented in:

[`docs/architecture.md`](docs/architecture.md)


## Project Structure

```text
nyc_taxi_final_project/
├── .github/
│   └── workflows/
│       └── ci.yml
├── dags/
│   ├── nyc_taxi_full_rebuild_pipeline.py
│   ├── nyc_taxi_period_refresh_pipeline.py
│   └── nyc_taxi_process_new_months_pipeline.py
├── jobs/
│   ├── config and runtime validation
│   ├── raw period discovery
│   ├── Bronze, Silver, and Gold Spark jobs
│   ├── data quality checks
│   ├── ClickHouse utilities and load jobs
│   └── Airflow failure callbacks
├── tests/
│   ├── configuration and utility tests
│   ├── Airflow DAG structure tests
│   ├── Spark transformation tests
│   ├── data quality tests
│   └── destructive-operation safety tests
├── data/
│   └── geo/
├── docs/
│   ├── analytics_summary.md
│   ├── architecture.md
│   ├── data_quality.md
│   ├── monitoring_plan.md
│   ├── testing.md
│   ├── roadmap.md
│   └── nyc_taxi_bi_dashboard.pdf
├── sql/
│   └── analytics/
├── screenshots/
├── superset/
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

Key directories:

- `dags/` — Airflow orchestration for full rebuild, period refresh, and new-month processing;
- `jobs/` — Spark ETL, validation, discovery, ClickHouse, and callback modules;
- `tests/` — automated unit and Airflow structure tests;
- `docs/` — monitoring, analytics, and portfolio documentation;
- `sql/analytics/` — analytical SQL queries;
- `superset/` — exported Superset dashboard artifacts;
- `screenshots/` — portfolio screenshots of Airflow, ClickHouse, and Superset.


## Data Source

The project uses historical NYC Yellow Taxi trip records in Parquet format.

The current raw source contains 120 consecutive monthly files:

```text
2016-01 → 2025-12
```

Monthly files are stored in S3-compatible Object Storage using the following contract:

```text
nyc_taxi/raw/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
```

Example:

```text
nyc_taxi/raw/yellow/year=2024/month=05/yellow_tripdata_2024-05.parquet
```

The year and month in the directory path must match the values in the filename.

A taxi-zone lookup is also stored in the raw layer:

```text
nyc_taxi/raw/lookup/taxi_zone_lookup.csv
```

It is used to enrich pickup and dropoff location identifiers with readable zone metadata.

### Raw Period Discovery

Raw discovery is implemented in:

```text
jobs/raw_discovery.py
```

The module:

* lists monthly Yellow Taxi objects;
* validates raw paths and filename periods;
* ignores malformed or unrelated objects;
* normalizes, deduplicates, and sorts periods;
* detects missing and unexpected periods;
* compares raw periods with periods loaded into ClickHouse.

The discovery rules depend on the processing mode:

| Processing mode        | Discovery rule                                                                    |
| ---------------------- | --------------------------------------------------------------------------------- |
| Protected full rebuild | discovered raw periods must exactly match the explicitly confirmed expected range |
| New-month processing   | select raw periods not fully present in all ClickHouse Gold tables                |

A month is considered fully processed only when it exists in all four Gold tables:

```text
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

This prevents partially loaded months from being incorrectly skipped.

Detailed discovery and orchestration logic is documented in:

[`docs/architecture.md`](docs/architecture.md)


## Data Pipeline

The pipeline follows a medallion architecture:

```text
raw → bronze → silver → gold → ClickHouse → Superset
```

| Layer  | Grain and purpose                            | Main processing                                                                                           |
| ------ | -------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Raw    | original monthly Yellow Taxi files           | immutable source Parquet data and taxi-zone lookup                                                        |
| Bronze | source-level records with technical metadata | ingestion, load timestamp, source system, year, and month                                                 |
| Silver | cleaned trip-level records                   | validation, standardization, derived dates and hours, trip duration, trip type, and bad-record separation |
| Gold   | aggregated analytical marts                  | daily, hourly, payment, and pickup/dropoff route metrics                                                  |

Main Spark jobs:

```text
jobs/bronze_yellow_taxi.py
jobs/silver_yellow_taxi.py
jobs/gold_daily_trips.py
jobs/gold_hourly_trips.py
jobs/gold_payment_type_stats.py
jobs/gold_location_pair_stats.py
```

Bronze, Silver, and Gold datasets are partitioned by processing period where applicable and stored in S3-compatible Object Storage.

Gold marts are validated before being loaded into ClickHouse.

Detailed data-flow and processing-mode architecture is documented in:

[`docs/architecture.md`](docs/architecture.md)


## Data Quality

The pipeline applies validation gates at three stages:

```text
Silver data
        │
        ▼
Gold Object Storage
        │
        ▼
ClickHouse serving layer
```

### Silver Quality

Silver validation checks source and transformed records before analytical marts are created.

Main controls include:

- required timestamps and analytical fields;
- valid date boundaries for the selected month;
- positive distance and duration values;
- supported payment types;
- valid pickup and dropoff location identifiers;
- valid pickup-hour values;
- row-count and row-loss checks.

Invalid records are written to the bad-records layer and excluded from downstream Gold processing.

### Gold Object Storage Quality

Before loading data into ClickHouse, the Gold quality gate verifies:

- expected Parquet datasets exist and can be read;
- required columns are present;
- marts are not empty;
- monthly date boundaries are correct;
- trip type, payment, hour, and zone fields are valid.

ClickHouse load tasks do not run when the Gold quality gate fails.

### ClickHouse Month-Level Quality

After all four Gold marts are loaded for a period, the pipeline validates that:

- all expected ClickHouse tables exist;
- every table contains rows for the selected month;
- dates belong to the expected period;
- trip counts are positive;
- hourly, payment, route, and trip-type dimensions are valid.

The month-level quality check is used by all three Airflow processing scenarios:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
```

A period is considered successfully processed only after this final serving-layer validation passes.

Detailed validation rules, failure behavior, and test coverage are documented in:

[`docs/data_quality.md`](docs/data_quality.md)



## Gold Analytical Marts

The pipeline builds four business-ready Gold marts:

| Mart | Analytical grain | Main use cases |
|---|---|---|
| `gold_daily_trips` | one row per pickup date | daily trip volume, revenue, average check, distance, duration, and trip-type distribution |
| `gold_hourly_trips` | pickup date, pickup hour, and trip type | hourly demand, peak-hour analysis, revenue, and trip behavior by time of day |
| `gold_payment_type_stats` | pickup date, payment type, and trip type | payment preferences, revenue, average check, tips, and payment trends |
| `gold_location_pair_stats` | pickup date, pickup/dropoff route, and trip type | route demand, route revenue, zone analysis, and grouped-ride opportunities |

Gold marts are created by:

```text
jobs/gold_daily_trips.py
jobs/gold_hourly_trips.py
jobs/gold_payment_type_stats.py
jobs/gold_location_pair_stats.py
```

Each mart is:

```text
written to S3-compatible Object Storage
        │
        ▼
validated by the Gold quality gate
        │
        ▼
loaded into ClickHouse
```

The hourly, payment, and route marts include the derived `trip_type` dimension:

```text
short
medium
long
```

The route mart is enriched with readable pickup and dropoff zone metadata from the NYC Taxi zone lookup.

Detailed schemas and column-level documentation can be maintained separately in dbt documentation or a dedicated data-model document.


## ClickHouse Serving Layer

ClickHouse is used as the analytical serving layer for Superset and ad-hoc SQL queries.

Database:

```text
nyc_taxi
```

Gold tables:

```text
nyc_taxi.gold_daily_trips
nyc_taxi.gold_hourly_trips
nyc_taxi.gold_payment_type_stats
nyc_taxi.gold_location_pair_stats
```

The shared table list is defined in `jobs/config.py` as:

```text
GOLD_CLICKHOUSE_TABLES
```

It is reused by loading, discovery, validation, cleanup, and full rebuild utilities.

### Monthly Loads

Spark-based load jobs read one monthly Gold Parquet dataset from Object Storage and append it to the corresponding ClickHouse table:

```text
jobs/load_gold_daily_trips_to_clickhouse.py
jobs/load_gold_hourly_trips_to_clickhouse.py
jobs/load_gold_payment_type_stats_to_clickhouse.py
jobs/load_gold_location_pair_stats_to_clickhouse.py
```

Before loading, each job verifies that the source dataset is not empty. After loading, the selected monthly period is validated in ClickHouse.

### Cleanup Strategy

Because ClickHouse loads use append mode, existing data is removed before reprocessing.

| Pipeline mode | Cleanup behavior |
|---|---|
| Protected full rebuild | truncate all configured Gold tables once |
| Period refresh | delete only the selected month |
| New-month processing | delete the discovered month, including partial data if present |

Cleanup jobs:

```text
jobs/truncate_clickhouse_gold_tables.py
jobs/delete_clickhouse_gold_month.py
```

### Shared ClickHouse Utilities

Common HTTP execution logic is centralized in:

```text
jobs/clickhouse_utils.py
```

It provides:

- authentication;
- HTTP query execution;
- timeout and connection handling;
- error reporting;
- JSON response parsing;
- reusable result validation.

The module is reused by ClickHouse setup, cleanup, validation, lookup loading, and SQL execution jobs.

### Serving-Layer Availability

The current local full rebuild truncates the existing Gold tables before monthly reload begins.

During a full rebuild:

- ClickHouse is gradually repopulated period by period;
- Superset may temporarily show incomplete historical data;
- a failed run can leave the serving layer partially loaded.

A production-oriented improvement would load into staging tables and publish the completed dataset with an atomic table swap.

Detailed pipeline orchestration is described in the [Airflow Orchestration](#airflow-orchestration) section.

Detailed serving-layer validation rules are documented in [`docs/data_quality.md`](docs/data_quality.md).


## ClickHouse Output

ClickHouse stores the validated analytical output produced by the Airflow pipelines.

A monthly period is considered successfully loaded only when:

- all four Gold marts contain data for the period;
- the month-level ClickHouse quality check passes;
- the period is present in every configured Gold table.

### Validated Historical Full Rebuild

The protected full rebuild was successfully completed for the complete available raw source:

```text
periods:       120
date range:    2016-01 → 2025-12
DAG state:     success
runtime:       23 h 42 min 37 sec
````

Post-run validation confirmed:

```text
raw periods:                       120
fully processed ClickHouse periods: 120
missing periods:                   none
unexpected periods:                none
```

Final ClickHouse row counts:

```text
gold_daily_trips                 3,653
gold_hourly_trips                262,969
gold_payment_type_stats          51,993
gold_location_pair_stats         39,368,728
```

The final row counts matched the serving-layer baseline captured before the rebuild, confirming reproducible output after the complete historical recalculation.

### Historical 2024 Optimization Benchmark

The earlier validated 2024-only pipeline produced:

```text
date range:                     2024-01-01 → 2024-12-31
gold_daily_trips:              366 rows
gold_hourly_trips:             26,349 rows
gold_payment_type_stats:       5,490 rows
gold_location_pair_stats:      3,460,697 rows
```

Its runtime decreased from approximately 2 hours 2 minutes to 1 hour 39 minutes after the Spark and quality-check optimization pass.

The 2024 values are retained as a historical performance benchmark and are not the totals for the complete historical dataset.

Serving-layer validation queries and the post-rebuild checklist are documented in:

[`docs/full_rebuild_runbook.md`](docs/full_rebuild_runbook.md)



## Airflow Orchestration

Apache Airflow orchestrates three processing scenarios:

| DAG | Period selection | ClickHouse cleanup | Purpose |
|---|---|---|---|
| `nyc_taxi_full_rebuild_pipeline` | all raw periods validated against an explicitly confirmed range | truncate all Gold tables once | rebuild the complete serving layer |
| `nyc_taxi_period_refresh_pipeline` | operator-defined month or limited interval | delete each selected month | safely replace existing periods |
| `nyc_taxi_process_new_months_pipeline` | raw periods not fully present in all Gold tables | delete discovered month or partial data | process new and incomplete periods |

### Protected Full Rebuild

The full rebuild DAG discovers available raw periods at runtime and validates them before ClickHouse can be truncated.

Its pre-truncation safety chain is:

```text
validate explicit confirmations
        │
        ▼
discover raw periods
        │
        ▼
validate raw source against the confirmed range
        │
        ▼
log rebuild plan
        │
        ▼
truncate ClickHouse Gold tables
```

The current raw source contains 120 consecutive periods covering:

```text
2016-01 → 2025-12
```

The DAG uses Airflow dynamic task mapping instead of hard-coded years and months.

### Shared Monthly Pipeline

All three DAGs reuse the same monthly processing stages:

```text
Bronze
→ Silver
→ Silver quality
→ four Gold marts
→ Gold Object Storage quality
→ four ClickHouse loads
→ ClickHouse month-level quality
```

Period-based DAGs dynamically map this flow through a monthly `process_month` TaskGroup.

A period is considered successfully processed only after its final ClickHouse month-level quality check passes.

### Resource and Failure Controls

The local Docker setup uses:

```text
max_active_runs = 1
max_active_tasks = 1
spark_pool slots = 1
```

These settings keep local Spark execution sequential and prevent Spark-heavy tasks from different DAGs from competing for resources.

Airflow tasks also use:

- one retry with a centralized retry delay;
- task-family-specific execution timeouts;
- a shared failure callback;
- structured failure messages in Airflow logs;
- optional Telegram failure notifications.

Detailed orchestration and safety behavior are documented in:

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/data_quality.md`](docs/data_quality.md)
- [`docs/monitoring_plan.md`](docs/monitoring_plan.md)

Runtime trigger examples are provided in the [How to Run Locally](#how-to-run-locally) section.


## BI and Analytical Layer

Apache Superset is used as the visualization and exploratory analytics layer on top of ClickHouse Gold marts.

Dashboard:

```text
NYC Taxi BI Dashboard
```

The dashboard includes:

* executive KPIs for trips, revenue, average check, distance, and duration;
* daily trip and revenue trends;
* hourly demand and peak-hour analysis;
* short, medium, and long trip comparisons;
* payment preference and tip analytics;
* top pickup and dropoff zones;
* route-level demand and revenue analysis;
* pickup and dropoff demand maps;
* grouped-ride opportunity analysis for high-volume short routes.

A `pickup_date` filter allows users to analyze selected reporting periods.

### Virtual Datasets and Geospatial Enrichment

Some charts query ClickHouse Gold marts directly, while others use Superset virtual datasets for chart-specific transformations.

Geospatial maps use taxi-zone centroid coordinates stored in:

```text
data/geo/taxi_zone_centroids.csv
```

The lookup is loaded into:

```text
nyc_taxi.taxi_zone_centroids
```

Superset virtual datasets deduplicate multi-part taxi-zone geometries to one row per `location_id` before joining them with aggregated demand. This prevents duplicated metrics in map visualizations.

### Analytical Artifacts

Analytical SQL queries are stored in:

```text
sql/analytics/
```

The business analysis and recommendations are documented in:

[`docs/analytics_summary.md`](docs/analytics_summary.md)

The exported dashboard PDF is available at:

[`docs/nyc_taxi_bi_dashboard.pdf`](docs/nyc_taxi_bi_dashboard.pdf)

Versioned Superset export artifacts are stored in:

```text
superset/
```

The analysis covers demand, revenue, trip type, payment behavior, routes, zones, geospatial patterns, and grouped-ride opportunities.


## Automated Tests and CI

The project includes more than 300 automated tests covering:

- project and runtime configuration;
- period generation and validation;
- raw Object Storage discovery;
- protected full rebuild safety;
- Airflow DAG imports, dependencies, and dynamic task mapping;
- PySpark Silver and Gold transformations;
- Silver, Gold, and ClickHouse quality gates;
- ClickHouse HTTP utilities and cleanup logic;
- Airflow failure callbacks and Telegram alerting.

Potentially destructive ClickHouse operations are mocked in unit tests.

The test suite does not execute real:

```text
TRUNCATE TABLE
ALTER TABLE ... DELETE
```

queries against the project database.

### Runtime Safety Validation

The protected full rebuild safeguards were also validated with real Airflow negative runs:

```text
missing explicit confirmations
→ configuration validation failed
→ ClickHouse truncation did not run
```

```text
confirmed range did not match Object Storage
→ raw source validation failed
→ ClickHouse truncation did not run
```

ClickHouse row counts remained unchanged after both runs.

### Running Tests

Run the complete suite locally:

```bash
PYTHONPATH=jobs python -m pytest tests -v
```

Run it inside the active Airflow container:

```bash
docker compose exec airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest tests -v"
```

Run tests in a temporary container without starting or resuming the main Airflow scheduler:

```bash
docker compose run --rm airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest tests -v"
```

### GitHub Actions CI

GitHub Actions runs on pushes and pull requests.

Workflow:

```text
.github/workflows/ci.yml
```

CI performs:

- Python syntax validation;
- complete `pytest` execution;
- Airflow DAG import and dependency checks;
- Spark transformation tests;
- data-quality tests;
- full rebuild safety tests;
- ClickHouse utility and cleanup tests;
- callback and Telegram tests with mocked external calls.

Detailed test coverage and targeted test commands are documented in:

[`docs/testing.md`](docs/testing.md)



## Pipeline Optimization

The pipeline was optimized to reduce redundant Spark work, repeated Object Storage reads, and unnecessary full-data scans without changing business logic.

Main improvements:

* consolidated repeated data-quality actions into aggregate checks;
* reduced redundant Spark `count()` and `show()` operations;
* removed unnecessary sorting before Parquet writes;
* reused intermediate Silver quality results with `StorageLevel.DISK_ONLY`;
* avoided caching large DataFrames that increased Java heap pressure;
* replaced full Parquet counts with lightweight non-empty checks;
* validated ClickHouse loads only for the selected `year` and `month`;
* centralized ClickHouse HTTP logic in `clickhouse_utils.py`;
* ensured Spark sessions are stopped with `try/finally` cleanup.

Historical local benchmark for the original 2024 full-year pipeline:

```text
before optimization: approximately 2 h 02 min
after optimization:  approximately 1 h 39 min
improvement:         approximately 23 min / 18.9%
```

The largest gains came from reducing repeated Spark actions and consolidating quality-check scans.

The benchmark documents the earlier 2024 pipeline and is not an estimate for the complete `2016-01 → 2025-12` historical rebuild.

## Monitoring and Alerting

The Airflow layer includes production-like reliability and failure-monitoring controls.

Implemented controls:

* one retry with a centralized retry delay;
* task-family-specific execution timeouts;
* a shared fallback timeout for future task types;
* structured Airflow failure callbacks;
* failure details written to task logs;
* optional Telegram failure notifications;
* safe handling of Telegram delivery errors.

The shared callback records:

```text
DAG ID
task ID
run ID
try number
logical date
exception details
Airflow log URL
```

Telegram alerting is disabled by default and can be enabled through environment variables:

```text
TELEGRAM_ALERTS_ENABLED
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
TELEGRAM_API_TIMEOUT_SECONDS
```

If a task exceeds its configured `execution_timeout`, Airflow fails the task and invokes the shared failure callback.

The current implementation focuses on task failures and hard execution limits. Runtime trend monitoring and alerts for tasks that are still running are planned improvements.

Detailed monitoring thresholds, alert behavior, and incident-response guidance are documented in:

[`docs/monitoring_plan.md`](docs/monitoring_plan.md)


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

Open the Airflow web interface:

```text
http://localhost:8080
```

Available DAGs:

| DAG | Use case |
|---|---|
| `nyc_taxi_full_rebuild_pipeline` | rebuild all validated historical periods |
| `nyc_taxi_period_refresh_pipeline` | replace one month or a limited interval |
| `nyc_taxi_process_new_months_pipeline` | process new or partially loaded raw months |

#### Protected Full Rebuild

The full rebuild discovers raw periods at runtime and allows ClickHouse truncation only after explicit confirmation and exact raw-range validation.

Trigger config for the current source:

```json
{
  "rebuild_mode": "full_raw_rebuild",
  "confirm_full_rebuild": true,
  "confirm_clickhouse_truncate": true,
  "expected_start_year": "2016",
  "expected_start_month": "01",
  "expected_end_year": "2025",
  "expected_end_month": "12"
}
```

The current implementation truncates active Gold tables before monthly loading begins, so Superset may temporarily show incomplete data.

Follow the complete preflight, execution, monitoring, recovery, and validation procedure in:

[`docs/full_rebuild_runbook.md`](docs/full_rebuild_runbook.md)

#### Period Refresh

Use `nyc_taxi_period_refresh_pipeline` to safely replace one month or a limited interval without truncating the full serving layer.

Example one-month refresh:

```json
{
  "start_year": "2024",
  "start_month": "05",
  "end_year": "2024",
  "end_month": "05",
  "refresh_mode": "replace_period"
}
```

For each selected month, the DAG deletes the existing ClickHouse period, rebuilds the monthly pipeline, reloads all four Gold marts, and runs the month-level quality check.

#### New-Month Processing

Use `nyc_taxi_process_new_months_pipeline` to process raw periods that are not fully present in all ClickHouse Gold tables.

The DAG requires no period config. It discovers eligible months automatically, cleans any partial ClickHouse data, processes each period, and validates the result.

If every raw period is already fully processed, the DAG completes successfully as a no-op.

Detailed processing-mode architecture is documented in:

[`docs/architecture.md`](docs/architecture.md)


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

### Superset Dashboard

| Executive overview | Trip types and peak demand |
|---|---|
| ![Superset Executive Overview](screenshots/superset_01_executive_overview.png) | ![Superset Trip Type and Peak Demand](screenshots/superset_02_trip_type_and_peak_demand.png) |

| Heatmap and geospatial maps | Demand zones |
|---|---|
| ![Superset Heatmap and Geospatial Maps](screenshots/superset_03_heatmap_and_geo_maps.png) | ![Superset Geospatial Demand and Top Zones](screenshots/superset_04_geo_demand_zones.png) |

| Routes and payment trends | Payment analytics |
|---|---|
| ![Superset Routes and Payment Trends](screenshots/superset_05_routes_and_payment_trends.png) | ![Superset Payment Analytics](screenshots/superset_06_payment_analytics.png) |

| Grouped-ride opportunities |
|---|
| ![Superset Ridesharing Opportunities](screenshots/superset_07_ridesharing_opportunities.png) |

### Pipeline Execution and Serving Layer

| Airflow pipeline | ClickHouse Gold tables |
|---|---|
| ![Airflow Successful Pipeline Run](screenshots/airflow_successful_run_graph.png) | ![ClickHouse Gold Tables](screenshots/clickhouse_gold_tables.png) |



## Roadmap

The next major development phase is a dbt analytical modeling layer.

Planned improvements:

- add dbt sources, staging models, analytical marts, tests, documentation, and lineage;
- add a final full-range quality gate confirming that every expected period exists in all ClickHouse Gold tables;
- introduce ClickHouse staging tables and atomic publication for safer full rebuilds;
- add resumable full rebuild execution and recovery procedures;
- add ClickHouse schema migration and versioning;
- add runtime trend monitoring and alerts for long-running tasks;
- migrate Spark execution to a managed cloud runtime;
- add Terraform-based infrastructure and managed secrets;
- verify Superset dashboard import on a clean environment;
- add polygon-based taxi-zone maps and historical comparison dashboards.

Detailed future-development plans are maintained in:

[`docs/roadmap.md`](docs/roadmap.md)