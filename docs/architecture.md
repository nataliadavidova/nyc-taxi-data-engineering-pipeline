# NYC Taxi Pipeline Architecture

This document describes the architecture, processing modes, storage layers, orchestration logic, and reliability controls used by the NYC Taxi data engineering pipeline.

## Architecture Overview

The project processes historical NYC Yellow Taxi data using a medallion-style batch architecture:

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
ClickHouse Serving Layer
        │
        ▼
Apache Superset
```

The current raw source contains:

```text
120 monthly periods
2016-01 → 2025-12
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

Airflow orchestrates three processing scenarios:

```text
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
```

All three DAGs reuse the same monthly transformation and loading jobs but differ in:

* how periods are selected;
* how existing ClickHouse data is cleaned;
* which safety checks run before processing.

### ClickHouse

ClickHouse is the analytical serving layer.

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

### Apache Superset

Superset queries ClickHouse marts and provides:

* executive KPIs;
* daily and hourly demand trends;
* payment analytics;
* route and zone analysis;
* geospatial visualizations;
* grouped-ride opportunity analysis.

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

## Airflow Processing Modes

## 1. Protected Full Rebuild

DAG:

```text
nyc_taxi_full_rebuild_pipeline
```

Purpose:

```text
rebuild the complete ClickHouse serving layer
from every validated raw monthly period
```

### Period Selection

The DAG discovers all available Yellow Taxi raw periods from Object Storage.

It does not contain a hard-coded year or static month list.

For the current source:

```text
2016-01 → 2025-12
120 periods
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
  "expected_end_year": "2025",
  "expected_end_month": "12"
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
  "start_year": "2024",
  "start_month": "01",
  "end_year": "2024",
  "end_month": "02",
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

If no new periods are found, the DAG completes successfully as a no-op.

## Shared Monthly Pipeline

All three DAGs reuse the same monthly processing stages:

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

## Data Quality Integration

Quality gates are part of task dependencies rather than separate reporting-only checks.

A downstream stage cannot start when its upstream validation fails.

Detailed validation rules are documented in:

```text
docs/data_quality.md
```

## Testing and CI

Architecture and orchestration behavior are covered by:

* DAG import tests;
* task dependency tests;
* dynamic mapping tests;
* period config tests;
* raw discovery tests;
* destructive-operation safety tests;
* transformation tests;
* data quality tests.

Detailed testing documentation should be maintained in:

```text
docs/testing.md
```

GitHub Actions runs syntax checks and the complete automated test suite on pushes and pull requests.

## Future Architecture Improvements

Planned production-like improvements include:

* dbt analytical modeling;
* staging ClickHouse tables;
* atomic publication of rebuilt datasets;
* resumable full rebuild execution;
* full-range post-rebuild validation;
* ClickHouse schema migrations;
* remote Spark job submission;
* cloud infrastructure managed with Terraform;
* centralized secrets management;
* historical runtime and freshness monitoring.