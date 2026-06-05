# NYC Taxi Pipeline Monitoring and Alerting Plan

## Purpose

This document describes a production-like monitoring and alerting strategy for the NYC Taxi data engineering pipeline.

The current project is a local portfolio implementation, but the monitoring design follows production data platform principles:

- detect failed pipeline runs;
- detect unusually slow Spark jobs;
- validate row counts and data completeness;
- prevent bad data from reaching BI dashboards;
- monitor the ClickHouse analytical serving layer;
- ensure Superset dashboards remain usable and trustworthy.

## Pipeline Overview

The project processes full-year 2024 NYC Yellow Taxi data using the following architecture:

```text
raw → bronze → silver → gold → ClickHouse → Superset
```

Main components:

- **Object Storage** stores raw, bronze, silver, gold, bad records, and quality reports;
- **Spark** performs batch data processing and transformations;
- **Airflow** orchestrates monthly pipeline tasks and dependencies;
- **ClickHouse** stores business-ready analytical marts for BI;
- **Superset** provides dashboards and data exploration;
- **GitHub Actions** validates configuration helpers, DAG imports, and Airflow task dependencies.

## Current Runtime Baseline

The current runtime baseline is based on the latest successful full-year local Airflow DAG run after the optimization pass.

| Metric | Value |
|---|---:|
| Full DAG runtime before optimization | ~2h 02m |
| Full DAG runtime after optimization | ~1h 39m |
| Runtime reduction | ~23 minutes |
| Relative improvement | ~18.9% |

The latest successful optimized DAG run completed in approximately:

```text
99.66 minutes
```

These values are based on local Docker execution and should be recalibrated if the pipeline is moved to cloud or production infrastructure.

## Task Runtime Baseline

The table below summarizes task duration statistics from the latest successful full-year DAG run.

| Task family | Tasks | Median runtime, min | Max runtime, min |
|---|---:|---:|---:|
| `silver_yellow_taxi` | 12 | 3.50 | 4.30 |
| `gold_location_pair_stats` | 12 | 1.76 | 2.05 |
| `bronze_yellow_taxi` | 12 | 1.38 | 1.66 |
| `gold_payment_type_stats` | 12 | 0.93 | 1.08 |
| `gold_hourly_trips` | 12 | 0.86 | 1.02 |
| `gold_daily_trips` | 12 | 0.85 | 0.99 |
| `check_gold_schema` | 12 | 0.52 | 0.89 |
| `check_yellow_taxi_quality` | 12 | 0.57 | 0.83 |
| `load_gold_location_pair_stats_to_clickhouse` | 12 | 0.41 | 0.48 |
| `load_gold_payment_type_stats_to_clickhouse` | 12 | 0.34 | 0.42 |
| `load_gold_daily_trips_to_clickhouse` | 12 | 0.35 | 0.42 |
| `load_gold_hourly_trips_to_clickhouse` | 12 | 0.35 | 0.41 |
| `check_clickhouse_gold_quality` | 1 | 0.01 | 0.01 |

## SLA Thresholds

The SLA thresholds below are initial local thresholds based on the latest successful full-year DAG run.

They are intentionally higher than the observed maximum values to avoid noisy alerts, but low enough to catch meaningful runtime regressions.

| Area | Warning threshold | Critical threshold |
|---|---:|---:|
| Full DAG runtime | > 115 min | > 135 min |
| `silver_yellow_taxi` monthly task | > 6.5 min | > 10 min |
| `gold_location_pair_stats` monthly task | > 3.5 min | > 5 min |
| `bronze_yellow_taxi` monthly task | > 2.5 min | > 4 min |
| `gold_daily_trips` monthly task | > 1.5 min | > 2.5 min |
| `gold_hourly_trips` monthly task | > 2 min | > 3 min |
| `gold_payment_type_stats` monthly task | > 2 min | > 3 min |
| `check_yellow_taxi_quality` monthly task | > 1.5 min | > 2 min |
| `check_gold_schema` monthly task | > 1.5 min | > 2 min |
| `load_gold_*_to_clickhouse` monthly tasks | > 1 min | > 2 min |
| `check_clickhouse_gold_quality` | > 1 min | > 2 min |

### Current Airflow Execution Timeouts

The project currently uses baseline Airflow execution timeouts as a first hardening layer:

```text
SPARK_TASK_EXECUTION_TIMEOUT_MINUTES = 30
PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES = 10
```

Spark-heavy tasks use the Spark timeout. This includes Bronze, Silver, Silver quality, Gold mart, Gold Object Storage schema/quality, and Spark-based ClickHouse load tasks.

Lightweight Python/ClickHouse tasks use the Python timeout. This includes ClickHouse table creation, truncation, month deletion, runtime config reading, raw period discovery, logging tasks, and ClickHouse quality checks.

These execution timeouts are intentionally broader than the SLA thresholds above.

The SLA thresholds are monitoring signals:

```text
warning threshold  → investigate runtime drift
critical threshold → serious runtime anomaly
```

Airflow execution timeout is a hard stop:

```text
execution_timeout → fail the task if it appears to be hanging
```

A future improvement is to replace the current broad task-category timeouts with task-family-specific execution timeouts based on the SLA thresholds in this document.

## Airflow Monitoring

Airflow should be the primary orchestration monitoring layer.

### What to Monitor

| Metric | Description | Expected Result |
|---|---|---|
| DAG run status | Whether the full DAG finished successfully | `success` |
| Task status | Whether each task finished successfully | `success` |
| Task retries | Number of retries per task | Usually `0` |
| Task duration | Runtime of each Airflow task | Within SLA threshold |
| DAG duration | Total runtime of the full-year DAG | Around current baseline |
| Failed task logs | Error details for failed tasks | Actionable logs available |

### Important Airflow Tasks

The most important tasks to monitor are:

- `bronze_yellow_taxi_*`
- `silver_yellow_taxi_*`
- `check_yellow_taxi_quality_*`
- `gold_daily_trips_*`
- `gold_hourly_trips_*`
- `gold_payment_type_stats_*`
- `gold_location_pair_stats_*`
- `check_gold_schema_*`
- `load_gold_*_to_clickhouse_*`
- `check_clickhouse_gold_quality`

### Alert Conditions

| Severity | Condition | Action |
|---|---|---|
| Critical | DAG failed | Investigate failed task logs immediately |
| Critical | `check_clickhouse_gold_quality` failed | BI serving layer may be incomplete or invalid |
| High | Any Silver job failed | Investigate data quality or Spark processing |
| High | Any ClickHouse load job failed | Serving layer may be incomplete |
| Medium | Any task retried | Review logs and runtime |
| Medium | DAG runtime above warning threshold | Investigate slow task families |

### Implemented Airflow Failure Alerting

The project includes an implemented Airflow failure alerting layer for local production-like monitoring.

All NYC Taxi DAGs use a shared Airflow failure callback:

```text
jobs/airflow_callbacks.py
```

The callback builds a structured failure message from the Airflow task context and writes it to Airflow task logs.

The failure message includes:

- DAG ID;
- task ID;
- Airflow run ID;
- try number;
- logical date;
- Airflow task log URL;
- exception details.

The same message can also be sent to Telegram when Telegram alerting is enabled and configured.

Telegram alerting is controlled through environment variables:

```text
TELEGRAM_ALERTS_ENABLED=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_API_TIMEOUT_SECONDS=10
```

By default, Telegram alerting is disabled so the project can run locally without external secrets.

For local manual testing, Telegram alerting was enabled through the private `.env` file, and the Airflow failure callback successfully delivered a test notification to Telegram.

The callback is designed to be safe:

- it always writes the failure message to Airflow logs;
- it sends Telegram alerts only when alerting is enabled and fully configured;
- it catches Telegram delivery errors so alerting failures do not hide the original Airflow task failure;
- unit tests mock Telegram delivery and do not send real messages.

## Spark Job Runtime Monitoring

Spark task duration should be tracked per job and per month.

### Silver Job SLA

The Silver job is the most important monthly transformation step because it performs data quality checks, bad record extraction, cleaning, and feature derivation.

Current observed Silver runtime:

```text
min:    3.24 min
median: 3.50 min
max:    4.30 min
```

Initial Silver SLA:

```text
warning:  Silver monthly task runtime > 6.5 minutes
critical: Silver monthly task runtime > 10 minutes
```

A Silver runtime above the warning or critical threshold may indicate:

- Object Storage latency;
- unexpected source data volume increase;
- inefficient Spark execution plan;
- data skew;
- excessive bad records;
- Spark memory pressure;
- infrastructure resource contention.

### Gold Location Pair SLA

The `gold_location_pair_stats` job is the heaviest Gold mart job because it builds route-level pickup/dropoff statistics.

Current observed runtime:

```text
median: 1.76 min
max:    2.05 min
```

Initial SLA:

```text
warning:  > 3.5 minutes
critical: > 5 minutes
```

## Data Quality Monitoring

Data quality is checked at several layers:

1. Silver layer quality checks;
2. Gold Object Storage schema and quality checks;
3. ClickHouse final serving-layer quality checks;
4. Superset dashboard validation.

## Silver Layer Quality Monitoring

The Silver job validates row-level data and writes:

- cleaned Silver data;
- bad records;
- quality report.

Important checks include:

- pickup datetime is not null;
- dropoff datetime is not null;
- pickup date belongs to the expected month;
- dropoff time is after pickup time;
- trip distance is positive;
- fare amount is non-negative;
- total amount is non-negative;
- passenger count is valid when present;
- payment type is valid;
- pickup/dropoff location IDs are valid;
- trip duration is positive and not extreme;
- extreme trip distances are flagged.

### Silver Data Quality Alert Rules

| Severity | Condition | Action |
|---|---|---|
| Critical | Silver output is empty | Stop pipeline and investigate |
| Critical | Rows outside expected month > 0 | Stop pipeline |
| Critical | Invalid pickup hour > 0 | Stop pipeline |
| High | Silver rows < 70% of Bronze rows | Investigate excessive filtering |
| Medium | Bad records share spikes compared with previous months | Investigate source data quality |

## Gold Object Storage Monitoring

The `check_gold_schema.py` job validates Gold parquet marts before loading them into ClickHouse.

It checks:

- expected columns exist;
- Gold marts are not empty;
- pickup dates belong to the expected processing month;
- `trip_type` is not empty where required;
- `pickup_hour` is valid for hourly trips;
- `payment_type_name` is not empty for payment type stats;
- pickup/dropoff zones are not empty for location pair stats.

### Gold Object Storage Alert Rules

| Severity | Condition | Action |
|---|---|---|
| Critical | Any Gold mart is empty | Stop ClickHouse load |
| Critical | Missing expected columns | Fix transformation/schema mismatch |
| Critical | Pickup dates outside expected month | Investigate upstream Silver data |
| High | Empty zone names in location mart | Check taxi zone lookup enrichment |
| High | Empty payment type names | Check payment type mapping |

## ClickHouse Serving Layer Monitoring

ClickHouse is the final analytical serving layer used by Superset.

The final quality gate is:

```text
check_clickhouse_gold_quality.py
```

It validates that:

- all expected gold tables exist;
- all gold tables are not empty;
- pickup date range covers the expected full year;
- `trip_type` is populated in relevant marts;
- location zone names are populated;
- payment type names are populated.

### Expected ClickHouse Tables

| Table | Purpose |
|---|---|
| `gold_daily_trips` | Daily taxi demand and revenue metrics |
| `gold_hourly_trips` | Hourly demand by trip type |
| `gold_payment_type_stats` | Payment behavior and tips |
| `gold_location_pair_stats` | Pickup/dropoff route-level statistics |
| `taxi_zone_centroids` | Geospatial lookup for Superset maps |

### Expected Full-Year Date Range

```text
2024-01-01 → 2024-12-31
```

### ClickHouse Alert Rules

| Severity | Condition | Action |
|---|---|---|
| Critical | Any expected gold table is missing | Re-run table creation job |
| Critical | Any gold table is empty | Investigate load jobs |
| Critical | Full-year date range is incomplete | Re-run missing monthly loads |
| High | Location zones are empty | Check lookup enrichment |
| High | Payment type names are empty | Check payment mapping |
| Medium | Row counts are lower than expected | Compare Object Storage Gold and ClickHouse |

## Taxi Zone Centroid Lookup Monitoring

The `taxi_zone_centroids` table is a static reference table used for Superset map charts.

The loader validates:

- source CSV exists;
- table has the expected number of rows;
- known duplicate `location_id` values are monitored;
- `borough` and `zone` fields are not empty;
- latitude and longitude values are within a reasonable NYC range.

### Expected Values

| Metric | Expected Value |
|---|---:|
| Rows count | 263 |
| Duplicate `location_id` count | 3 |
| Empty borough count | 0 |
| Empty zone count | 0 |

Duplicate `location_id` values are expected because some NYC taxi zones are represented by multiple geometry parts. These duplicates should be handled carefully in Superset virtual datasets to avoid duplicating demand metrics.

## Superset Dashboard Monitoring

Superset dashboards depend on the ClickHouse serving layer.

### What to Monitor

| Metric | Description |
|---|---|
| Dashboard opens successfully | User can open the dashboard |
| Charts load successfully | No chart-level query errors |
| ClickHouse connection works | Superset can query ClickHouse |
| Map charts render correctly | Latitude/longitude lookup works |
| Filters work | Dashboard filters return expected results |

### Alert Conditions

| Severity | Condition | Action |
|---|---|---|
| High | Dashboard cannot load | Check Superset and ClickHouse |
| High | Charts show query errors | Check dataset SQL and table availability |
| Medium | Map charts do not render | Check `taxi_zone_centroids` |
| Medium | Dashboard data looks incomplete | Run ClickHouse quality checks |

## Alerting Strategy

In a production setup, alerts should be sent when failures affect the reliability of the pipeline or BI layer.

The current project implements the first alerting layer directly in Airflow:

- task failure messages are written to Airflow logs through a shared failure callback;
- optional Telegram alerts can be enabled through environment variables;
- Telegram delivery errors are caught and logged so they do not mask the original task failure.

Implemented alerting channels:

- Airflow task logs;
- optional Telegram notifications for Airflow task failures.

Future alerting channels may include:

- Slack notifications;
- Airflow email alerts;
- monitoring dashboards in Grafana;
- incident tickets for repeated failures.

### Suggested Alert Levels

| Level | Examples |
|---|---|
| Critical | DAG failed, ClickHouse final quality check failed, missing tables |
| High | Silver job failed, ClickHouse load failed, incomplete date range |
| Medium | Slow Silver job, high bad records share, task retries |
| Low | Minor runtime drift, non-blocking warnings |

## Incident Response

When a pipeline issue occurs, the recommended response flow is:

1. Identify the failed Airflow task.
2. Open task logs in Airflow.
3. Determine the layer where the failure happened:
    - Bronze;
    - Silver;
    - Gold;
    - ClickHouse load;
    - final quality check;
    - Superset dashboard.
4. Check relevant Object Storage path or ClickHouse table.
5. Re-run the failed task if the issue was transient.
6. Re-run the affected month if the issue corrupted downstream data.
7. Re-run the full DAG if ClickHouse was truncated or the serving layer is incomplete.
8. Document the incident and update thresholds if needed.

## Future Monitoring Improvements

Future improvements may include:

- task-family-specific Airflow execution timeouts based on the SLA thresholds documented in this file;
- Slack or additional external notification channels on top of the existing failure callback;
- Airflow task-level SLA configuration;
- automatic runtime threshold checks;
- historical task duration tracking;
- Prometheus and Grafana dashboards;
- ClickHouse row count trend monitoring;
- automated comparison between Object Storage Gold and ClickHouse rows;
- alerting on bad records share spikes;
- Superset dashboard availability checks;
- data freshness checks for BI users.

## Summary

This monitoring plan defines how to track the health of the NYC Taxi data pipeline across orchestration, Spark processing, data quality validation, ClickHouse serving tables, and Superset dashboards.

The most important monitoring priorities are:

1. Airflow DAG success/failure with structured failure messages and optional Telegram alerts;
2. monthly Silver job runtime SLA;
3. Silver and Gold data quality checks;
4. final ClickHouse full-year quality check;
5. Superset dashboard availability.