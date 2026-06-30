# NYC Taxi Data Engineering Pipeline

End-to-end data engineering project for processing historical NYC Yellow Taxi trip data using a medallion lake architecture, Apache Spark ETL jobs, Airflow orchestration, ClickHouse as an analytical serving layer, dbt as an analytics modeling and testing layer, and Apache Superset for BI dashboards.

The current raw dataset contains 121 monthly Yellow Taxi files covering:

```text
2016-01 → 2026-01
```

The project supports three Airflow processing scenarios:

* protected full rebuild of all validated raw periods;
* safe month-level and interval refreshes;
* automated processing of newly discovered raw months.

After each successful data-processing scenario, Airflow triggers the dbt analytics pipeline to rebuild and validate the downstream analytical layer.

## Project Overview

The project implements a production-like batch data platform for historical NYC Yellow Taxi data.

Monthly Parquet files are read from S3-compatible Object Storage and processed through raw, bronze, silver, and gold layers. Validated Gold marts are loaded into ClickHouse, transformed into dbt analytical models, tested with dbt data checks, and exposed through Apache Superset.

The platform supports three Airflow processing modes:

* protected full rebuild of all validated raw periods;
* safe replacement of one month or a limited interval;
* automated processing of newly discovered or partially loaded months.

The implementation combines PySpark ETL, Airflow dynamic task mapping, multi-layer data quality gates, ClickHouse serving, dbt analytics modeling, Docker-based infrastructure, automated testing, CI, and failure alerting.



## Project Highlights

* Built an end-to-end batch data engineering pipeline for 121 monthly NYC Yellow Taxi periods covering `2016-01 → 2026-01`.
* Implemented a medallion architecture with raw, bronze, silver, and gold layers using PySpark and Parquet.
* Created four ClickHouse Gold marts for daily performance, hourly demand, payment behavior, and pickup/dropoff route analysis.
* Added a dbt analytics layer on top of ClickHouse with sources, staging models, an intermediate model, a daily KPI mart, and data tests.
* Loaded business-ready marts into ClickHouse and built an Apache Superset dashboard for operational, financial, and geospatial analytics.
* Implemented three Airflow processing scenarios:

    * protected full rebuild of all validated raw periods;
    * safe month-level and interval replacement;
    * automated processing of newly discovered raw months.
* Orchestrated dbt from Airflow so that successful full rebuild, period refresh, and new-month processing runs trigger the downstream analytics build.
* Replaced the original static 2024 pipeline with dynamically mapped DAGs that select processing periods at runtime.
* Protected destructive full rebuilds with explicit operator confirmations, expected-range validation, missing-period detection, and unexpected-period detection before ClickHouse truncation.
* Validated both pre-truncation safety gates with real Airflow negative runs; ClickHouse row counts remained unchanged when confirmation or raw-range validation failed.
* Added Silver, Gold Object Storage, ClickHouse month-level, and dbt analytics quality gates to prevent invalid or incomplete data from reaching the BI layer.
* Used dbt tests to detect duplicated analytical grains in an existing historical period and fixed the issue through safe period refresh without weakening validation rules.
* Added production-like reliability controls with retries, task-specific execution timeouts, a shared Spark pool, structured failure callbacks, Docker health checks, and optional Telegram alerts.
* Added more than 300 automated tests and GitHub Actions CI covering transformations, DAG structure, dynamic mapping, raw discovery, ClickHouse utilities, dbt orchestration, and destructive-operation safety.
* Optimized Spark and quality-check workloads, reducing the historical 2024 full-pipeline benchmark from approximately 2 hours 2 minutes to 1 hour 39 minutes.


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

| Component                    | Role                                                                                         |
| ---------------------------- | -------------------------------------------------------------------------------------------- |
| Python / PySpark             | ETL jobs, validation, discovery, and utility scripts                                         |
| Apache Spark                 | distributed batch processing                                                                 |
| Apache Airflow               | orchestration, retries, timeouts, dynamic task mapping, dbt triggering, and failure handling |
| S3-compatible Object Storage | Raw, Bronze, Silver, Gold, bad-record, and quality datasets                                  |
| Parquet                      | columnar storage format for lake layers                                                      |
| ClickHouse                   | analytical serving layer for validated Gold marts and dbt models                             |
| dbt Core / dbt-clickhouse    | analytics modeling, staging, marts, SQL-based transformations, and data tests                |
| Apache Superset              | dashboards and exploratory analytics                                                         |
| Docker Compose               | reproducible local infrastructure                                                            |
| PyTest                       | automated unit, transformation, and pipeline-structure tests                                 |
| GitHub Actions               | continuous integration                                                                       |
| SQL                          | ClickHouse validation, dbt models, and analytical queries                                    |

High-level data flow:

```text
S3-compatible Object Storage
        │
        ▼
Raw → Bronze → Silver → Gold
                              │
                              ▼
                        ClickHouse Gold
                              │
                              ▼
                         dbt Analytics
                              │
                              ▼
                        Apache Superset
```

Pipeline orchestration:

```text
Apache Airflow
→ selects processing periods
→ runs Spark jobs
→ applies lake and serving-layer quality gates
→ loads ClickHouse Gold marts
→ triggers dbt analytics build
→ validates dbt models and data tests
→ handles retries, timeouts, and failures
```

The dbt analytics layer is orchestrated by a dedicated Airflow DAG:

```text
nyc_taxi_dbt_analytics_pipeline
```

It is triggered automatically after successful runs of:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
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
│   ├── nyc_taxi_dbt_analytics_pipeline.py
│   ├── nyc_taxi_full_rebuild_pipeline.py
│   ├── nyc_taxi_period_refresh_pipeline.py
│   └── nyc_taxi_process_new_months_pipeline.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/
│   ├── airflow/
│   └── dbt/
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
│   ├── dbt orchestration tests
│   ├── Spark transformation tests
│   ├── data quality tests
│   └── destructive-operation safety tests
├── data/
│   └── geo/
├── docs/
│   ├── analytics_summary.md
│   ├── architecture.md
│   ├── data_quality.md
│   ├── full_rebuild_runbook.md
│   ├── monitoring_plan.md
│   ├── testing.md
│   ├── roadmap.md
│   └── nyc_taxi_bi_dashboard.pdf
├── sql/
│   └── analytics/
├── screenshots/
├── superset/
├── docker-compose.yml
├── .env.example
└── README.md
```

Key directories:

* `dags/` — Airflow orchestration for full rebuild, period refresh, new-month processing, and dbt analytics builds;
* `dbt/` — dbt sources, staging models, intermediate models, marts, and data tests;
* `docker/` — custom Docker images for Airflow and dbt runtime environments;
* `jobs/` — Spark ETL, validation, discovery, ClickHouse, and callback modules;
* `tests/` — automated unit, transformation, data-quality, Airflow structure, and dbt orchestration tests;
* `docs/` — architecture, runbooks, monitoring, analytics, testing, roadmap, and portfolio documentation;
* `sql/analytics/` — analytical SQL queries;
* `superset/` — exported Superset dashboard artifacts;
* `screenshots/` — portfolio screenshots of Airflow, ClickHouse, and Superset.



## Data Source

The project uses historical NYC Yellow Taxi trip records in Parquet format.

The current raw source contains 121 consecutive monthly files:

```text
2016-01 → 2026-01
```

Monthly files are stored in S3-compatible Object Storage using the following contract:

```text
nyc_taxi/raw/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
```

Example:

```text
nyc_taxi/raw/yellow/year=2026/month=01/yellow_tripdata_2026-01.parquet
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

A month is considered fully processed only when it exists in all four ClickHouse Gold tables:

```text
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

This prevents partially loaded months from being incorrectly skipped.

After new raw data is processed into ClickHouse Gold, Airflow triggers the dbt analytics pipeline so the downstream analytical models stay synchronized with the serving layer.

Detailed discovery and orchestration logic is documented in:

[`docs/architecture.md`](docs/architecture.md)

## Data Pipeline

The pipeline follows a medallion architecture with an additional dbt analytics layer:

```text
raw → bronze → silver → gold → ClickHouse Gold → dbt analytics → Superset
```

| Layer           | Grain and purpose                            | Main processing                                                                                           |
| --------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Raw             | original monthly Yellow Taxi files           | immutable source Parquet data and taxi-zone lookup                                                        |
| Bronze          | source-level records with technical metadata | ingestion, load timestamp, source system, year, and month                                                 |
| Silver          | cleaned trip-level records                   | validation, standardization, derived dates and hours, trip duration, trip type, and bad-record separation |
| Gold            | aggregated analytical marts                  | daily, hourly, payment, and pickup/dropoff route metrics                                                  |
| ClickHouse Gold | serving tables for validated Gold marts      | fast analytical storage for BI, SQL checks, period discovery, and dbt sources                             |
| dbt analytics   | curated downstream analytical models         | sources, staging models, intermediate transformations, marts, and data tests                              |
| Superset        | BI and exploratory analytics                 | dashboards, filters, charts, and reporting views                                                          |

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

The dbt layer reads validated ClickHouse Gold tables as sources and builds downstream analytical models in:

```text
nyc_taxi_analytics_dbt
```

The dbt analytics pipeline currently includes:

```text
sources → staging models → intermediate model → mart_daily_trip_kpis
```

dbt data tests validate model grain, required fields, accepted values, and business consistency checks before the analytics layer is considered ready for BI consumption.

Detailed data-flow and processing-mode architecture is documented in:

[`docs/architecture.md`](docs/architecture.md)



## Data Quality

The pipeline applies validation gates at four stages:

```text
Silver data
        │
        ▼
Gold Object Storage
        │
        ▼
ClickHouse serving layer
        │
        ▼
dbt analytics layer
```

### Silver Quality

Silver validation checks source and transformed records before analytical marts are created.

Main controls include:

* required timestamps and analytical fields;
* valid date boundaries for the selected month;
* positive distance and duration values;
* supported payment types;
* valid pickup and dropoff location identifiers;
* valid pickup-hour values;
* row-count and row-loss checks.

Invalid records are written to the bad-records layer and excluded from downstream Gold processing.

### Gold Object Storage Quality

Before loading data into ClickHouse, the Gold quality gate verifies:

* expected Parquet datasets exist and can be read;
* required columns are present;
* marts are not empty;
* monthly date boundaries are correct;
* trip type, payment, hour, and zone fields are valid.

ClickHouse load tasks do not run when the Gold quality gate fails.

### ClickHouse Month-Level Quality

After all four Gold marts are loaded for a period, the pipeline validates that:

* all expected ClickHouse tables exist;
* every table contains rows for the selected month;
* dates belong to the expected period;
* trip counts are positive;
* hourly, payment, route, and trip-type dimensions are valid.

The month-level quality check is used by all three data-processing Airflow scenarios:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
```

A monthly period is considered successfully processed only after this final serving-layer validation passes.

### dbt Analytics Quality

After ClickHouse Gold is successfully updated, Airflow triggers the dbt analytics pipeline:

```text
nyc_taxi_dbt_analytics_pipeline
```

The dbt layer validates downstream analytical models using:

* source definitions for ClickHouse Gold tables;
* staging models with normalized analytical fields;
* intermediate business transformations;
* curated mart models;
* not-null and uniqueness checks;
* accepted-value checks;
* custom grain and consistency tests.

The current dbt build includes:

```text
sources → staging models → intermediate model → mart_daily_trip_kpis
```

A successful dbt build confirms that the downstream analytics layer is consistent with the latest ClickHouse Gold data before it is used by BI.

During implementation, dbt tests detected duplicated analytical grains in the historical `2021-09` period. The issue was fixed through the safe `nyc_taxi_period_refresh_pipeline` without weakening validation rules. After the refresh, the dbt build passed again with all checks green.

Detailed validation rules, failure behavior, and test coverage are documented in:

[`docs/data_quality.md`](docs/data_quality.md)

## Gold Analytical Marts

The Spark pipeline builds four business-ready Gold marts:

| Mart                       | Analytical grain                                 | Main use cases                                                                            |
| -------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `gold_daily_trips`         | one row per pickup date                          | daily trip volume, revenue, average check, distance, duration, and trip-type distribution |
| `gold_hourly_trips`        | pickup date, pickup hour, and trip type          | hourly demand, peak-hour analysis, revenue, and trip behavior by time of day              |
| `gold_payment_type_stats`  | pickup date, payment type, and trip type         | payment preferences, revenue, average check, tips, and payment trends                     |
| `gold_location_pair_stats` | pickup date, pickup/dropoff route, and trip type | route demand, route revenue, zone analysis, and grouped-ride opportunities                |

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
        │
        ▼
used as a dbt source
```

The hourly, payment, and route marts include the derived `trip_type` dimension:

```text
short
medium
long
```

The route mart is enriched with readable pickup and dropoff zone metadata from the NYC Taxi zone lookup.

The ClickHouse Gold marts serve as stable source tables for the dbt analytics layer.

## ClickHouse Serving Layer

ClickHouse is used as the analytical serving layer for validated Spark Gold marts, dbt analytical models, Superset dashboards, and ad-hoc SQL queries.

Source Gold database:

```text
nyc_taxi
```

Gold source tables:

```text
nyc_taxi.gold_daily_trips
nyc_taxi.gold_hourly_trips
nyc_taxi.gold_payment_type_stats
nyc_taxi.gold_location_pair_stats
```

dbt analytics database:

```text
nyc_taxi_analytics_dbt
```

Main dbt analytical mart:

```text
nyc_taxi_analytics_dbt.mart_daily_trip_kpis
```

The shared Spark Gold table list is defined in `jobs/config.py` as:

```text
GOLD_CLICKHOUSE_TABLES
```

It is reused by loading, discovery, validation, cleanup, and full rebuild utilities.

### Monthly Loads

Spark-based load jobs read one monthly Gold Parquet dataset from Object Storage and append it to the corresponding ClickHouse Gold table:

```text
jobs/load_gold_daily_trips_to_clickhouse.py
jobs/load_gold_hourly_trips_to_clickhouse.py
jobs/load_gold_payment_type_stats_to_clickhouse.py
jobs/load_gold_location_pair_stats_to_clickhouse.py
```

Before loading, each job verifies that the source dataset is not empty. After all four marts are loaded for a selected month, the month-level ClickHouse quality check validates the complete serving-layer period.

### Cleanup Strategy

Because ClickHouse Gold loads use append mode, existing data is removed before reprocessing.

| Pipeline mode          | Cleanup behavior                                               |
| ---------------------- | -------------------------------------------------------------- |
| Protected full rebuild | truncate all configured Gold tables once                       |
| Period refresh         | delete only the selected month                                 |
| New-month processing   | delete the discovered month, including partial data if present |

Cleanup jobs:

```text
jobs/truncate_clickhouse_gold_tables.py
jobs/delete_clickhouse_gold_month.py
```

This makes period refreshes and new-month processing idempotent at the monthly serving-table level.

### Shared ClickHouse Utilities

Common HTTP execution logic is centralized in:

```text
jobs/clickhouse_utils.py
```

It provides:

* authentication;
* HTTP query execution;
* timeout and connection handling;
* error reporting;
* JSON response parsing;
* reusable result validation.

The module is reused by ClickHouse setup, cleanup, validation, lookup loading, and SQL execution jobs.

### dbt Analytics Layer

The dbt project reads validated ClickHouse Gold tables as sources and builds curated analytics models in a separate database:

```text
dbt/
```

The dbt pipeline currently includes:

```text
sources
→ staging models
→ intermediate model
→ mart_daily_trip_kpis
→ dbt data tests
```

Airflow runs the dbt layer through:

```text
nyc_taxi_dbt_analytics_pipeline
```

This DAG executes:

```text
dbt debug
→ dbt build
```

The dbt analytics DAG is triggered automatically after successful runs of:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
```

For `nyc_taxi_process_new_months_pipeline`, the dbt trigger runs only when at least one new or incomplete raw period was discovered and processed. If there are no new periods, the DAG completes successfully as a no-op and skips the dbt trigger.

### Serving-Layer Availability

The current local full rebuild truncates the existing Gold tables before monthly reload begins.

During a full rebuild:

* ClickHouse Gold is gradually repopulated period by period;
* dbt analytics models are rebuilt only after the full rebuild succeeds;
* Superset may temporarily show incomplete historical data while the full rebuild is running;
* a failed full rebuild can leave the serving layer partially loaded.

A production-oriented improvement would load into staging tables and publish the completed dataset with an atomic table swap.

Detailed pipeline orchestration is described in the [Airflow Orchestration](#airflow-orchestration) section.

Detailed serving-layer and dbt validation rules are documented in [`docs/data_quality.md`](docs/data_quality.md).


## ClickHouse Output

ClickHouse stores the validated analytical output produced by the Airflow pipelines.

A monthly period is considered successfully loaded only when:

* all four Gold marts contain data for the period;
* the month-level ClickHouse quality check passes;
* the period is present in every configured Gold table.

The dbt analytics layer is rebuilt after successful data-processing runs and stores curated downstream models in a separate ClickHouse database.

### Current Validated Serving-Layer Output

The current validated serving-layer output covers the complete available raw source:

```text
raw periods:                  121
date range:                   2016-01 → 2026-01
ClickHouse Gold periods:      121
dbt analytics mart periods:   121
```

The serving layer was built through a combination of:

```text
protected full rebuild        2016-01 → 2025-12
new-month processing          2026-01
safe period refresh           2021-09
dbt analytics rebuilds        after successful data-processing runs
```

Current ClickHouse Gold row counts:

```text
gold_daily_trips                 3,684
gold_hourly_trips                265,201
gold_payment_type_stats          52,458
gold_location_pair_stats         39,815,396
```

Current Gold date coverage:

```text
min_date                         2016-01-01
max_date                         2026-01-31
periods_count                    121
```

Current dbt analytics mart coverage:

```text
database                         nyc_taxi_analytics_dbt
mart                             mart_daily_trip_kpis
rows_count                       3,684
min_date                         2016-01-01
max_date                         2026-01-31
periods_count                    121
```

The dbt analytics build currently validates the downstream analytical layer after ClickHouse Gold is updated. During implementation, dbt tests detected duplicated analytical grains in the historical `2021-09` period. The issue was resolved through safe period refresh, and the dbt build passed again after the corrected month was reloaded.

### Validated Historical Full Rebuild

The protected full rebuild was successfully completed for the initial complete historical source:

```text
periods:       120
date range:    2016-01 → 2025-12
DAG state:     success
runtime:       23 h 42 min 37 sec
```

Post-run validation confirmed:

```text
raw periods:                       120
fully processed ClickHouse periods: 120
missing periods:                   none
unexpected periods:                none
```

The final row counts matched the serving-layer baseline captured before the rebuild, confirming reproducible output after the complete historical recalculation.

After this validated full rebuild, the additional raw period `2026-01` was processed through the new-month pipeline and included in the current serving-layer output.

### Historical 2024 Optimization Benchmark

The earlier validated 2024-only pipeline produced:

```text
date range:                     2024-01-01 → 2024-12-31
gold_daily_trips:               366 rows
gold_hourly_trips:              26,349 rows
gold_payment_type_stats:        5,490 rows
gold_location_pair_stats:       3,460,697 rows
```

Its runtime decreased from approximately 2 hours 2 minutes to 1 hour 39 minutes after the Spark and quality-check optimization pass.

The 2024 values are retained as a historical performance benchmark and are not the totals for the complete historical dataset.

Serving-layer validation queries and the post-rebuild checklist are documented in:

[`docs/full_rebuild_runbook.md`](docs/full_rebuild_runbook.md)



## Airflow Orchestration

Apache Airflow orchestrates three data-processing scenarios and one downstream dbt analytics scenario:

| DAG                                    | Period selection                                                | ClickHouse cleanup                      | Downstream dbt behavior                                         | Purpose                                      |
| -------------------------------------- | --------------------------------------------------------------- | --------------------------------------- | --------------------------------------------------------------- | -------------------------------------------- |
| `nyc_taxi_full_rebuild_pipeline`       | all raw periods validated against an explicitly confirmed range | truncate all Gold tables once           | triggers dbt after successful full rebuild                      | rebuild the complete serving layer           |
| `nyc_taxi_period_refresh_pipeline`     | operator-defined month or limited interval                      | delete each selected month              | triggers dbt after successful refresh                           | safely replace existing periods              |
| `nyc_taxi_process_new_months_pipeline` | raw periods not fully present in all Gold tables                | delete discovered month or partial data | triggers dbt only when new or incomplete periods were processed | process new and incomplete periods           |
| `nyc_taxi_dbt_analytics_pipeline`      | reads validated ClickHouse Gold sources                         | no Spark Gold cleanup                   | runs `dbt debug` and `dbt build`                                | rebuild and validate the dbt analytics layer |

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

The current raw source contains 121 consecutive periods covering:

```text
2016-01 → 2026-01
```

The DAG uses Airflow dynamic task mapping instead of hard-coded years and months.

Because the full rebuild is destructive, it requires explicit runtime confirmation and exact expected raw-period boundaries before the `truncate_clickhouse_gold_tables` task can run.

### Period Refresh

The period refresh DAG is used to safely replace one month or a limited interval without truncating the complete serving layer.

For each selected month, the DAG:

```text
deletes the selected month from ClickHouse Gold
        │
        ▼
rebuilds Bronze, Silver, and Gold for that month
        │
        ▼
reloads all four ClickHouse Gold marts
        │
        ▼
runs ClickHouse month-level quality checks
        │
        ▼
triggers the dbt analytics pipeline
```

This mode is used for controlled backfills, corrections, and recovery from data-quality issues.

During implementation, duplicated analytical grains were detected in the historical `2021-09` period by dbt tests. The issue was fixed by running a safe `2021-09` period refresh, after which the dbt build passed again.

### New-Month Processing

The new-month DAG discovers raw periods that are not fully present in all four ClickHouse Gold tables.

A period is considered fully processed only when it exists in:

```text
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

If a raw month is missing from at least one Gold table, the DAG treats it as new or incomplete, removes any partial ClickHouse data for that month, rebuilds it, reloads all four Gold marts, and runs the month-level quality check.

If at least one period was processed successfully, the DAG triggers the dbt analytics pipeline.

If no new or incomplete periods are found, the DAG completes successfully as a no-op and skips the dbt trigger.

### Shared Monthly Pipeline

All three data-processing DAGs reuse the same monthly processing stages:

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

### dbt Analytics Orchestration

The dbt analytics DAG is responsible for rebuilding and validating the downstream analytical layer after ClickHouse Gold changes.

It runs:

```text
dbt debug
        │
        ▼
dbt build
```

The dbt build reads ClickHouse Gold tables as sources and creates analytical models in:

```text
nyc_taxi_analytics_dbt
```

The current dbt layer includes sources, staging models, an intermediate model, the `mart_daily_trip_kpis` mart, and dbt data tests.

The dbt analytics DAG is triggered automatically after successful runs of:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
```

This keeps the downstream analytics layer synchronized with the latest validated ClickHouse Gold data.

### Resource and Failure Controls

The local Docker setup uses:

```text
max_active_runs = 1
max_active_tasks = 1
spark_pool slots = 1
```

These settings keep local Spark execution sequential and prevent Spark-heavy tasks from different DAGs from competing for resources.

Airflow tasks also use:

* one retry with a centralized retry delay;
* task-family-specific execution timeouts;
* a shared Spark pool for Spark-heavy tasks;
* a shared failure callback;
* structured failure messages in Airflow logs;
* optional Telegram failure notifications;
* Docker health checks for infrastructure readiness.

Detailed orchestration and safety behavior are documented in:

* [`docs/architecture.md`](docs/architecture.md)
* [`docs/data_quality.md`](docs/data_quality.md)
* [`docs/monitoring_plan.md`](docs/monitoring_plan.md)

Runtime trigger examples are provided in the [How to Run Locally](#how-to-run-locally) section.


## BI and Analytical Layer

Apache Superset is used as the visualization and exploratory analytics layer on top of validated ClickHouse Gold marts.

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

### dbt Analytics Layer

The dbt analytics layer has been added downstream of ClickHouse Gold and is orchestrated by Airflow after successful data-processing runs.

It builds curated analytical models in a separate ClickHouse database:

```text
nyc_taxi_analytics_dbt
```

Main analytical mart:

```text
nyc_taxi_analytics_dbt.mart_daily_trip_kpis
```

The current dbt mart covers:

```text
2016-01-01 → 2026-01-31
```

with one row per pickup date.

At the current stage, Superset dashboards still query the original ClickHouse Gold marts directly. Connecting selected Superset datasets to dbt marts is a planned BI-layer improvement.

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

The dbt analytics project is stored in:

```text
dbt/
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

* project and runtime configuration;
* period generation and validation;
* raw Object Storage discovery;
* protected full rebuild safety;
* Airflow DAG imports, dependencies, dynamic task mapping, and trigger behavior;
* dbt analytics DAG configuration and Airflow orchestration;
* PySpark Silver and Gold transformations;
* Silver, Gold, ClickHouse, and dbt-related quality gates;
* ClickHouse HTTP utilities and cleanup logic;
* Airflow failure callbacks and Telegram alerting.

Potentially destructive ClickHouse operations are mocked in unit tests.

The test suite does not execute real:

```text
TRUNCATE TABLE
ALTER TABLE ... DELETE
```

queries against the project database.

### Airflow and dbt Orchestration Tests

The Airflow DAG tests validate that:

* the full rebuild DAG keeps destructive operations behind explicit safety gates;
* period refresh and new-month processing use dynamic task mapping correctly;
* Spark-heavy tasks use the shared `spark_pool`;
* task-family-specific execution timeouts are configured;
* all DAGs use the shared failure callback and retry settings;
* `nyc_taxi_dbt_analytics_pipeline` runs `dbt debug` before `dbt build`;
* full rebuild, period refresh, and new-month processing DAGs trigger the dbt analytics DAG after successful processing;
* new-month processing skips the dbt trigger when no new or incomplete raw periods are discovered.

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

Runtime end-to-end validation also confirmed:

```text
new raw period 2026-01
→ discovered by nyc_taxi_process_new_months_pipeline
→ processed through Spark and ClickHouse
→ triggered dbt analytics pipeline
→ dbt build passed
```

```text
duplicated analytical grains in 2021-09
→ detected by dbt tests
→ fixed through nyc_taxi_period_refresh_pipeline
→ dbt analytics pipeline triggered automatically
→ dbt build passed after refresh
```

The dbt build currently validates the analytics layer with 7 models, 5 sources, and 80 total checks/tests.

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

Run only the Airflow DAG structure tests:

```bash
docker compose exec airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest tests/test_full_rebuild_dag.py tests/test_period_refresh_dag.py tests/test_process_new_months_dag.py tests/test_dbt_analytics_dag.py -v"
```

Run dbt manually through Airflow for local validation:

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_debug 2026-01-03"
```

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_build_analytics_layer 2026-01-03"
```

### GitHub Actions CI

GitHub Actions runs on pushes and pull requests.

Workflow:

```text
.github/workflows/ci.yml
```

CI performs:

* Python syntax validation;
* complete `pytest` execution;
* Airflow DAG import and dependency checks;
* Spark transformation tests;
* data-quality tests;
* full rebuild safety tests;
* dbt orchestration structure tests;
* ClickHouse utility and cleanup tests;
* callback and Telegram tests with mocked external calls.

Runtime Spark execution, ClickHouse loads, and dbt builds are validated locally through Docker Compose and Airflow, while CI focuses on deterministic unit, transformation, utility, and DAG-structure checks.

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
* ensured Spark sessions are stopped with `try/finally` cleanup;
* separated downstream analytical modeling into dbt, so Spark Gold jobs and SQL analytics models can evolve independently.

Historical local benchmark for the original 2024 full-year pipeline:

```text
before optimization: approximately 2 h 02 min
after optimization:  approximately 1 h 39 min
improvement:         approximately 23 min / 18.9%
```

The largest gains came from reducing repeated Spark actions and consolidating quality-check scans.

The benchmark documents the earlier 2024 pipeline and is not an estimate for the complete current serving layer covering `2016-01 → 2026-01`.

## Monitoring and Alerting

The Airflow layer includes production-like reliability and failure-monitoring controls.

Implemented controls:

* one retry with a centralized retry delay;
* task-family-specific execution timeouts;
* a shared fallback timeout for future task types;
* a shared Spark pool for Spark-heavy tasks;
* structured Airflow failure callbacks;
* failure details written to task logs;
* optional Telegram failure notifications;
* safe handling of Telegram delivery errors;
* Postgres healthcheck in Docker Compose;
* dbt analytics DAG failures surfaced through Airflow task state and logs.

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

The dbt analytics pipeline is also orchestrated through Airflow. If `dbt debug` or `dbt build` fails, the corresponding Airflow task fails and the upstream data-processing DAG does not complete successfully when it is waiting for the dbt child DAG.

The current implementation focuses on task failures, hard execution limits, and downstream dbt build failures. Runtime trend monitoring and alerts for tasks that are still running are planned improvements.

Detailed monitoring thresholds, alert behavior, and incident-response guidance are documented in:

[`docs/monitoring_plan.md`](docs/monitoring_plan.md)


## How to Run Locally

### 1. Configure Environment Variables

Create a local `.env` file using `.env.example` as a template.

Required configuration includes:

* object storage bucket;
* S3-compatible endpoint;
* object storage credentials;
* ClickHouse credentials;
* dbt target database;
* Airflow admin credentials;
* Superset admin credentials;
* Superset secret key;
* Airflow webserver secret key.

The local `.env` file is not committed to Git and should contain only local development credentials.

### 2. Start Infrastructure

```bash
docker compose up -d --build
```

This starts:

* PostgreSQL for Airflow metadata;
* Airflow webserver and scheduler;
* ClickHouse;
* Superset;
* Docker-based runtime support for Spark and dbt jobs.

Check that services are running:

```bash
docker compose ps
```

### 3. Open Airflow

Open the Airflow web interface:

```text
http://localhost:8080
```

Available DAGs:

| DAG                                    | Use case                                                |
| -------------------------------------- | ------------------------------------------------------- |
| `nyc_taxi_full_rebuild_pipeline`       | rebuild all validated historical periods                |
| `nyc_taxi_period_refresh_pipeline`     | replace one month or a limited interval                 |
| `nyc_taxi_process_new_months_pipeline` | process new or partially loaded raw months              |
| `nyc_taxi_dbt_analytics_pipeline`      | rebuild and validate the downstream dbt analytics layer |

The dbt analytics DAG is triggered automatically after successful full rebuild, period refresh, and new-month processing runs.

#### Protected Full Rebuild

The full rebuild discovers raw periods at runtime and allows ClickHouse truncation only after explicit confirmation and exact raw-range validation.

Trigger config for the current raw source:

```json
{
  "rebuild_mode": "full_raw_rebuild",
  "confirm_full_rebuild": true,
  "confirm_clickhouse_truncate": true,
  "expected_start_year": "2016",
  "expected_start_month": "01",
  "expected_end_year": "2026",
  "expected_end_month": "01"
}
```

The current implementation truncates active Gold tables before monthly loading begins, so Superset may temporarily show incomplete data while the full rebuild is running.

After a successful full rebuild, Airflow triggers:

```text
nyc_taxi_dbt_analytics_pipeline
```

to rebuild and validate the downstream dbt analytics layer.

Follow the complete preflight, execution, monitoring, recovery, and validation procedure in:

[`docs/full_rebuild_runbook.md`](docs/full_rebuild_runbook.md)

#### Period Refresh

Use `nyc_taxi_period_refresh_pipeline` to safely replace one month or a limited interval without truncating the full serving layer.

Example one-month refresh:

```json
{
  "start_year": "2021",
  "start_month": "09",
  "end_year": "2021",
  "end_month": "09",
  "refresh_mode": "replace_period"
}
```

For each selected month, the DAG deletes the existing ClickHouse period, rebuilds the monthly pipeline, reloads all four Gold marts, and runs the month-level quality check.

After a successful period refresh, Airflow triggers the dbt analytics DAG so downstream analytical models are rebuilt from the corrected ClickHouse Gold data.

#### New-Month Processing

Use `nyc_taxi_process_new_months_pipeline` to process raw periods that are not fully present in all ClickHouse Gold tables.

The DAG requires no period config. It discovers eligible months automatically, cleans any partial ClickHouse data, processes each period, and validates the result.

If at least one raw period is processed, the DAG triggers the dbt analytics pipeline.

If every raw period is already fully processed, the DAG completes successfully as a no-op and skips the dbt trigger.

#### dbt Analytics Pipeline

The dbt analytics DAG can also be run manually for local validation:

```text
nyc_taxi_dbt_analytics_pipeline
```

It executes:

```text
dbt debug
→ dbt build
```

Manual task checks can be run from the Airflow container:

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_debug 2026-01-03"
```

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_build_analytics_layer 2026-01-03"
```

Detailed processing-mode architecture is documented in:

[`docs/architecture.md`](docs/architecture.md)

### 4. Open ClickHouse

ClickHouse HTTP interface:

```text
http://localhost:8123
```

Example query for Spark Gold tables:

```sql
SELECT *
FROM nyc_taxi.gold_daily_trips
LIMIT 10;
```

Example query for the dbt analytics mart:

```sql
SELECT *
FROM nyc_taxi_analytics_dbt.mart_daily_trip_kpis
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

At the current stage, Superset dashboards query the validated ClickHouse Gold marts directly. The dbt analytics layer is already built and validated downstream of ClickHouse Gold, and connecting selected Superset datasets to dbt marts is a planned BI-layer improvement.

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

The dbt analytical modeling layer has been implemented and is now orchestrated by Airflow after successful data-processing runs.

Planned improvements:

* connect selected Superset datasets and charts to dbt analytics marts;
* expand dbt models beyond `mart_daily_trip_kpis` with additional payment, hourly-demand, and route-level marts;
* add dbt documentation generation and model lineage artifacts;
* add a final full-range quality gate confirming that every expected period exists in all ClickHouse Gold tables and dbt marts;
* introduce ClickHouse staging tables and atomic publication for safer full rebuilds;
* add resumable full rebuild execution and recovery procedures;
* add ClickHouse schema migration and versioning;
* add runtime trend monitoring and alerts for long-running tasks;
* migrate Spark execution to a managed cloud runtime;
* add Terraform-based infrastructure and managed secrets;
* verify Superset dashboard import on a clean environment;
* add polygon-based taxi-zone maps and historical comparison dashboards.

Detailed future-development plans are maintained in:

[`docs/roadmap.md`](docs/roadmap.md)