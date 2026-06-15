# Project Roadmap

This document describes planned improvements for the NYC Taxi data engineering pipeline.

The roadmap is ordered from the nearest planned development phase to longer-term production-oriented improvements.

## Completed Milestone: Protected Historical Full Rebuild

The protected historical full rebuild was successfully completed for the full available source range:

```text
120 monthly periods
2016-01 → 2025-12
runtime: 23 h 42 min 37 sec
````

Post-run validation confirmed:

* all 120 raw periods were present in every ClickHouse Gold table;
* no expected periods were missing;
* no unexpected periods were loaded;
* final row counts matched the pre-rebuild serving-layer baseline;
* the Airflow DAG completed with `success`.

The next development phase therefore moves from validating the historical pipeline to improving analytical modeling, final full-range validation, schema management, and publication reliability.


## 1. dbt Analytical Modeling

The next major development phase is a dbt analytical modeling layer.

Planned work:

* configure dbt for ClickHouse;
* define existing ClickHouse Gold tables as dbt sources;
* create staging models for normalized analytical access;
* create business-facing mart models;
* add schema tests and custom data tests;
* generate dbt documentation and lineage;
* define model ownership and descriptions;
* decide which transformations should remain in Spark and which analytical logic should move to dbt;
* integrate dbt execution into Airflow;
* add dbt validation to GitHub Actions CI.

The intended responsibility split is:

```text
Spark
→ ingestion, cleaning, large-scale transformation, monthly Gold preparation

dbt
→ serving-layer transformations, analytical models, tests, documentation, lineage
```

## 2. Full-Range Serving-Layer Validation

The current pipelines validate every processed month independently.

A future final quality gate should validate the complete historical serving layer after all mapped monthly tasks have finished.

Planned checks:

* every expected period exists in all configured ClickHouse Gold tables;
* no periods are missing;
* no unexpected periods exist;
* minimum and maximum dates match the confirmed rebuild range;
* every table contains data;
* period counts are consistent across all Gold tables;
* Object Storage Gold periods match ClickHouse periods;
* final row counts and processing metadata are recorded.

For the current source, the expected result is:

```text
120 monthly periods
2016-01 → 2025-12
```

## 3. Safer Full Rebuild Publication

The current local full rebuild truncates the active ClickHouse Gold tables before loading begins.

This means the serving layer may be empty or partially populated while the rebuild is running.

A more production-oriented design should use:

```text
load staging tables
        │
        ▼
validate complete staging dataset
        │
        ▼
publish with atomic table swap
        │
        ▼
retain previous version for rollback
```

Planned improvements:

* create staging versions of all Gold tables;
* load the full rebuild into staging;
* run month-level and full-range quality checks;
* perform an atomic publication step;
* preserve the previous serving-layer version;
* add rollback support;
* prevent Superset from reading incomplete rebuild data.

## 4. Resumable Full Rebuilds

The current full rebuild processes all validated periods in one Airflow run.

Future improvements may include:

* recording completed periods;
* restarting from the first failed or incomplete period;
* avoiding unnecessary reprocessing of successful months;
* storing rebuild execution metadata;
* adding a dedicated recovery workflow;
* documenting recovery procedures for partial ClickHouse loads.

Suggested rebuild metadata:

```text
run_id
expected_start_period
expected_end_period
expected_period_count
completed_period_count
failed_periods
started_at
finished_at
status
```

## 5. ClickHouse Schema Management

Planned improvements:

* add versioned ClickHouse migrations;
* store schema changes in the repository;
* reuse `clickhouse_utils.py` as the common execution layer;
* validate compatibility between Spark Gold schemas and ClickHouse schemas;
* add migration checks to CI;
* document backward-incompatible changes;
* add rollback procedures where possible.

## 6. Monitoring and Observability

The current monitoring strategy includes retries, execution timeouts, structured failure callbacks, and optional Telegram alerts.

Detailed current behavior is documented in:

```text
docs/monitoring_plan.md
```

Future improvements:

* add informational alerts for tasks exceeding normal runtime;
* add critical alerts before tasks reach execution timeout;
* track historical task and DAG durations;
* monitor Airflow queue time and retry frequency;
* monitor ClickHouse row counts and period completeness;
* monitor source-data freshness;
* add Prometheus metrics;
* add Grafana dashboards;
* add Alertmanager;
* add Superset availability and query checks;
* support Slack or email notifications.

## 7. Cloud Execution

The current project runs Spark locally inside the Docker-based Airflow environment.

Future cloud work may include:

* migrate Spark workloads to Yandex Data Proc or another managed Spark runtime;
* submit remote Spark jobs from Airflow;
* poll remote job status;
* propagate cloud-job failures back to Airflow;
* separate orchestration from compute;
* use cloud Object Storage as the persistent data layer;
* evaluate managed Kafka and ClickHouse options for future projects.

## 8. Infrastructure as Code and Security

Planned infrastructure improvements:

* add Terraform;
* define cloud storage, compute, networking, and service configuration as code;
* create separate local, test, and cloud environments;
* move credentials to a managed secrets service;
* avoid long-lived credentials in local configuration;
* add role-based access controls;
* document backup and recovery procedures;
* extend CI/CD from validation to controlled deployment.

## 9. BI Reproducibility

Planned improvements:

* verify Superset dashboard import on a clean environment;
* maintain exported dashboards and datasets as versioned artifacts;
* document ClickHouse and Superset connection setup;
* add automated dashboard smoke tests;
* add data-freshness indicators;
* add historical period comparison views;
* update dashboard screenshots after the complete historical rebuild;
* improve portfolio documentation.

## 10. Geospatial Analytics

Current maps use taxi-zone centroid coordinates.

Future improvements:

* load official taxi-zone polygons;
* build polygon-based choropleth maps;
* improve multi-part geometry handling;
* compare pickup and dropoff demand geographically;
* add borough-level aggregations;
* add historical demand-change maps;
* evaluate spatial indexing and geospatial query support.

## 11. Project Documentation

Planned documentation improvements:

* create a full rebuild operational runbook;
* maintain architecture documentation;
* maintain testing documentation;
* document data models and table schemas;
* document incident and recovery procedures;
* create an interview-oriented project walkthrough;
* record final full rebuild runtime and final ClickHouse row counts.

Existing documents:

```text
docs/architecture.md
docs/data_quality.md
docs/testing.md
docs/monitoring_plan.md
docs/analytics_summary.md
```

## Priority Order

Current expected sequence:

```text
1. Add dbt analytical modeling
2. Add an automated full-range post-rebuild quality gate
3. Add ClickHouse schema migration and versioning
4. Improve monitoring and runtime observability
5. Introduce staging tables and atomic publication
6. Add resumable full rebuild execution and recovery metadata
7. Move Spark compute to a managed cloud runtime
8. Add Terraform and production-oriented infrastructure
```
