# NYC Taxi Pipeline Architecture

This document describes the architecture, processing modes, storage layers, orchestration logic, data quality integration, and reliability controls used by the NYC Taxi data engineering pipeline.

## Architecture Overview

The project processes historical NYC Yellow Taxi data using a medallion-style batch architecture with a downstream dbt analytics layer:

```text
S3-compatible Object Storage
        │
        ▼
Raw
        │
        ▼
Bronze
        │
        ▼
Silver + Data Quality
        │
        ▼
Gold Analytical Marts
        │
        ▼
ClickHouse Gold Serving Layer
        │
        ▼
dbt Analytics Layer
        │
        ▼
Apache Superset
```

At the current stage, Superset dashboards query the validated ClickHouse Gold marts directly. The dbt analytics layer is already built and orchestrated by Airflow, but selected Superset datasets have not yet been migrated to dbt marts.

The current raw source contains:

```text
121 monthly periods
2016-01 → 2026-01
```

## Main Components

### Object Storage

S3-compatible Object Storage is used for:

* raw monthly Yellow Taxi Parquet files;
* Bronze datasets;
* Silver datasets;
* bad records and quality outputs;
* Gold analytical marts;
* taxi zone lookup data.

Raw monthly files follow this structure:

```text
nyc_taxi/raw/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
```

Example:

```text
nyc_taxi/raw/yellow/year=2026/month=01/yellow_tripdata_2026-01.parquet
```

### Apache Spark

PySpark jobs implement:

* raw ingestion;
* technical metadata enrichment;
* data cleaning and standardization;
* bad-record separation;
* analytical feature derivation;
* Gold mart aggregation;
* Object Storage validation;
* ClickHouse loading.

### Apache Airflow

Airflow orchestrates three data-processing scenarios and one downstream dbt analytics scenario:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
nyc_taxi_dbt_analytics_pipeline
```

The three data-processing DAGs reuse the same monthly transformation and loading jobs but differ in:

* how periods are selected;
* how existing ClickHouse data is cleaned;
* which safety checks run before processing;
* whether dbt is triggered after successful processing.

The dbt analytics DAG is triggered automatically after successful full rebuild, period refresh, and new-month processing runs.

### ClickHouse

ClickHouse is used as the analytical serving layer for validated Gold marts and as the database for dbt analytics models.

Source Gold database:

```text
nyc_taxi
```

Configured Gold tables:

```text
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

The shared table list is defined as:

```text
GOLD_CLICKHOUSE_TABLES
```

in:

```text
jobs/config.py
```

dbt analytics database:

```text
nyc_taxi_analytics_dbt
```

Main dbt analytics mart:

```text
mart_daily_trip_kpis
```

### dbt

dbt Core with the ClickHouse adapter is used for downstream analytical modeling and SQL-based data validation.

The dbt project is stored in:

```text
dbt/
```

The current dbt pipeline includes:

```text
sources
→ staging models
→ intermediate model
→ mart_daily_trip_kpis
→ dbt data tests
```

The dbt layer reads validated ClickHouse Gold tables as sources and builds curated analytics models in a separate ClickHouse database.

Airflow runs dbt through:

```text
dbt debug
→ dbt build
```

### Apache Superset

Superset currently queries ClickHouse Gold marts and provides:

* executive KPIs;
* daily and hourly demand trends;
* payment analytics;
* route and zone analysis;
* geospatial visualizations;
* grouped-ride opportunity analysis.

Connecting selected Superset datasets and charts to dbt analytics marts is a planned BI-layer improvement.

## Data Layers

### Raw

The Raw layer contains original monthly Yellow Taxi Parquet files.

Raw data is immutable from the pipeline perspective.

### Bronze

Bronze preserves source data with minimal transformation and adds technical metadata such as:

* load timestamp;
* source system;
* source year;
* source month.

### Silver

Silver contains cleaned and standardized trip-level data.

It includes:

* validated timestamps;
* derived pickup date and hour;
* trip duration;
* normalized dimensions;
* trip type classification;
* valid records separated from bad records.

Detailed validation rules are documented in:

```text
docs/data_quality.md
```

### Gold

Gold contains aggregated business-ready marts.

| Mart                       | Grain                         | Purpose                               |
| -------------------------- | ----------------------------- | ------------------------------------- |
| `gold_daily_trips`         | pickup date                   | daily operational and revenue metrics |
| `gold_hourly_trips`        | date, hour, trip type         | demand and peak-hour analysis         |
| `gold_payment_type_stats`  | date, payment type, trip type | payment and tip analytics             |
| `gold_location_pair_stats` | date, route, trip type        | route and zone analytics              |

Gold marts are written to Object Storage, validated, and loaded into ClickHouse.

### ClickHouse Gold Serving Layer

The ClickHouse Gold serving layer stores validated analytical output from the Spark pipeline.

It is used for:

* BI dashboards;
* analytical SQL queries;
* serving-layer validation;
* raw-vs-processed period discovery;
* dbt source tables.

A monthly period is considered successfully loaded only when it exists in every configured ClickHouse Gold table and the month-level ClickHouse quality check passes.

### dbt Analytics Layer

The dbt analytics layer sits downstream of ClickHouse Gold.

It is used for:

* SQL-based analytics modeling;
* staging and normalization of analytical fields;
* downstream mart creation;
* dbt data tests;
* additional validation of analytical grain and business consistency.

The current dbt analytics mart covers:

```text
2016-01-01 → 2026-01-31
```

with one row per pickup date.

## Airflow Processing Modes

## 1. Protected Full Rebuild

DAG:

```text
nyc_taxi_full_rebuild_pipeline
```

Purpose:

```text
rebuild the complete ClickHouse Gold serving layer
from every validated raw monthly period
```

### Period Selection

The DAG discovers all available Yellow Taxi raw periods from Object Storage.

It does not contain a hard-coded year or static month list.

For the current raw source:

```text
2016-01 → 2026-01
121 periods
```

### Required Runtime Configuration

The full rebuild requires explicit operator confirmation:

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

Default values are intentionally non-destructive:

```text
rebuild_mode = ""
confirm_full_rebuild = false
confirm_clickhouse_truncate = false
expected period values = empty
```

### Pre-Truncation Safety Chain

```text
create ClickHouse Gold tables
        │
        ▼
validate explicit runtime configuration
        │
        ▼
discover raw Yellow Taxi periods
        │
        ▼
validate raw periods against expected range
        │
        ▼
log validated rebuild plan
        │
        ▼
truncate ClickHouse Gold tables
```

Truncation cannot run unless all previous tasks succeed.

Validation rejects the run when:

* rebuild mode is missing or incorrect;
* full rebuild confirmation is missing;
* ClickHouse truncation confirmation is missing;
* expected period values are missing;
* expected months are invalid;
* expected range is reversed;
* no raw periods are discovered;
* expected periods are missing from Object Storage;
* unexpected periods exist outside the confirmed range.

### Runtime Safety Validation

Two negative scenarios were validated with real Airflow runs.

Missing confirmations:

```text
validate_full_rebuild_config → failed
truncate_clickhouse_gold_tables → upstream_failed
```

Mismatched expected range:

```text
validate_full_rebuild_config → success
discover_full_rebuild_raw_periods → success
validate_full_rebuild_raw_periods → failed
truncate_clickhouse_gold_tables → upstream_failed
```

ClickHouse row counts remained unchanged in both scenarios.

### Monthly Processing

After successful validation and truncation, Airflow dynamically maps the monthly pipeline over all validated periods:

```text
process_month[0]
process_month[1]
...
```

Each mapped group runs:

```text
Bronze
→ Silver
→ Silver quality
→ four Gold marts
→ Gold Object Storage quality
→ four ClickHouse loads
→ ClickHouse month quality
```

Each period is considered complete only after its month-level ClickHouse quality task succeeds.

### Downstream dbt Trigger

After the full rebuild finishes successfully, Airflow triggers:

```text
nyc_taxi_dbt_analytics_pipeline
```

This rebuilds and validates the downstream dbt analytics layer from the refreshed ClickHouse Gold sources.

### Availability Limitation

The current local implementation truncates the existing serving tables before loading begins.

During execution:

* ClickHouse starts empty;
* data is restored month by month;
* Superset may display incomplete results;
* a failed run may leave the serving layer partially populated.

A future production-oriented design should use:

```text
staging tables
→ full validation
→ atomic table swap
```

## 2. Period Refresh

DAG:

```text
nyc_taxi_period_refresh_pipeline
```

Purpose:

```text
replace one month or a limited month interval
without truncating the full serving layer
```

Supported mode:

```text
replace_period
```

### Period Selection

Periods are passed through Airflow runtime configuration.

Example:

```json
{
  "start_year": "2021",
  "start_month": "09",
  "end_year": "2021",
  "end_month": "09",
  "refresh_mode": "replace_period"
}
```

The config helper validates:

* supported mode;
* year and month values;
* chronological range;
* maximum allowed interval size.

### Monthly Processing

For each selected period:

```text
delete selected ClickHouse month
→ Bronze
→ Silver
→ Silver quality
→ Gold marts
→ Gold quality
→ ClickHouse loads
→ month-level ClickHouse quality
```

The DAG uses dynamic task mapping and a mapped monthly TaskGroup.

### Cleanup Behavior

Existing rows for the selected month are deleted from all four ClickHouse Gold tables before reload.

This prevents duplicate rows while preserving all other periods.

### Downstream dbt Trigger

After a successful period refresh, Airflow triggers the dbt analytics pipeline.

This ensures that downstream analytical models are rebuilt from corrected ClickHouse Gold data.

During implementation, dbt tests detected duplicated analytical grains in the historical `2021-09` period. The issue was fixed by running the period refresh DAG for `2021-09`, after which the dbt build passed again.

## 3. New-Month Processing

DAG:

```text
nyc_taxi_process_new_months_pipeline
```

Purpose:

```text
process raw months that are not fully loaded in ClickHouse
```

### Period Selection

The DAG calculates:

```text
new periods =
raw Object Storage periods
-
periods fully present in all ClickHouse Gold tables
```

A period is fully processed only when it exists in every configured Gold table.

The intersection across all four tables protects against partially loaded months.

### Monthly Processing

For every discovered period:

```text
delete any existing partial ClickHouse month
→ Bronze
→ Silver
→ Silver quality
→ Gold marts
→ Gold quality
→ ClickHouse loads
→ month-level ClickHouse quality
```

For a truly new month, the delete step removes zero rows.

For a partially loaded month, it cleans inconsistent serving-layer data before rebuilding.

### Downstream dbt Trigger

If at least one new or incomplete raw period is discovered and processed successfully, the DAG triggers the dbt analytics pipeline.

If no new periods are found, the DAG completes successfully as a no-op and skips the dbt trigger.

The `2026-01` raw period was processed through this mode after the initial validated full rebuild for `2016-01 → 2025-12`.

## 4. dbt Analytics Pipeline

DAG:

```text
nyc_taxi_dbt_analytics_pipeline
```

Purpose:

```text
rebuild and validate the downstream dbt analytics layer
after ClickHouse Gold changes
```

The DAG runs:

```text
dbt debug
        │
        ▼
dbt build
```

`dbt debug` validates the runtime environment, project configuration, profile, and ClickHouse connectivity.

`dbt build` runs dbt models and data tests.

The current successful dbt build includes:

```text
7 models
5 sources
80 total checks/tests
```

The DAG can be triggered manually for local validation, but it is normally triggered by the three data-processing DAGs after successful processing.

## Shared Monthly Pipeline

The three data-processing DAGs reuse the same monthly processing stages:

```text
bronze_yellow_taxi
silver_yellow_taxi
check_yellow_taxi_quality
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
check_gold_schema
load_gold_*_to_clickhouse
check_clickhouse_gold_month_quality
```

This keeps transformation behavior consistent across:

* full rebuilds;
* selected period replacements;
* newly discovered months.

The downstream dbt build is intentionally separated from the monthly Spark pipeline. This keeps Spark Gold processing and SQL analytics modeling independent.

## Dynamic Task Mapping

Period-based pipelines use Airflow dynamic task mapping.

At DAG parse time, Airflow stores one TaskGroup definition:

```text
process_month
```

At runtime, Airflow creates mapped instances based on the selected period list:

```text
process_month[0]
process_month[1]
...
```

Benefits:

* no static task generation for specific years;
* the same DAG supports different period counts;
* raw discovery results can directly drive processing;
* DAG code remains independent of the available historical range.

## Resource Controls

The local environment uses:

```text
max_active_runs = 1
max_active_tasks = 1
```

for the period-based DAGs.

Spark-heavy tasks use an Airflow pool:

```text
spark_pool
slots = 1
```

These controls prevent:

* multiple full rebuild runs;
* concurrent monthly Spark tasks;
* Spark jobs from different NYC Taxi DAGs competing for local resources.

Docker Compose also defines infrastructure readiness checks, including a Postgres healthcheck used by Airflow services.

## Retries and Timeouts

Airflow runtime settings are centralized in:

```text
jobs/config.py
```

Tasks use:

* one retry;
* centralized retry delay;
* task-family-specific execution timeouts;
* a shared fallback timeout for future task families.

Timeouts act as hard stops for tasks that appear to be hanging.

Detailed monitoring thresholds are documented in:

```text
docs/monitoring_plan.md
```

## Failure Handling

All DAGs use the shared callback:

```text
jobs/airflow_callbacks.py
```

The callback records:

* DAG ID;
* task ID;
* run ID;
* logical date;
* try number;
* exception;
* Airflow log URL.

Messages are always written to Airflow task logs.

Optional Telegram delivery can be enabled through environment variables.

Alerting failures are caught and logged so they do not hide the original pipeline failure.

dbt failures are also surfaced through Airflow. If `dbt debug` or `dbt build` fails, the corresponding task fails and the upstream triggering DAG does not complete successfully when it waits for the dbt child DAG.

## Data Quality Integration

Quality gates are part of task dependencies rather than separate reporting-only checks.

A downstream stage cannot start when its upstream validation fails.

Validation exists at four levels:

```text
Silver data quality
        │
        ▼
Gold Object Storage quality
        │
        ▼
ClickHouse month-level quality
        │
        ▼
dbt analytics data tests
```

Detailed validation rules are documented in:

```text
docs/data_quality.md
```

## Testing and CI

Architecture and orchestration behavior are covered by:

* DAG import tests;
* task dependency tests;
* dynamic mapping tests;
* dbt orchestration tests;
* period config tests;
* raw discovery tests;
* destructive-operation safety tests;
* transformation tests;
* data quality tests;
* callback and alerting tests.

Detailed testing documentation is maintained in:

```text
docs/testing.md
```

GitHub Actions runs syntax checks and the complete automated test suite on pushes and pull requests.

Runtime Spark execution, ClickHouse loads, and dbt builds are validated locally through Docker Compose and Airflow, while CI focuses on deterministic unit, transformation, utility, and DAG-structure checks.

## Future Architecture Improvements

Planned production-like improvements include:

* connecting selected Superset datasets and charts to dbt analytics marts;
* expanding the dbt layer with additional payment, hourly-demand, and route-level marts;
* generating dbt documentation and lineage artifacts;
* staging ClickHouse tables;
* atomic publication of rebuilt datasets;
* resumable full rebuild execution;
* full-range post-rebuild validation across ClickHouse Gold and dbt marts;
* ClickHouse schema migrations;
* remote Spark job submission;
* cloud infrastructure managed with Terraform;
* centralized secrets management;
* historical runtime and freshness monitoring.