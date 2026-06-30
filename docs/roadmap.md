# Project Roadmap

This document describes completed milestones and planned improvements for the NYC Taxi data engineering pipeline.

The roadmap is ordered from already implemented portfolio milestones to longer-term production-oriented improvements.

## Completed Milestone: Protected Historical Full Rebuild

The protected historical full rebuild was successfully completed for the full raw source available at that time:

```text
120 monthly periods
2016-01 → 2025-12
runtime: 23 h 42 min 37 sec
```

Post-run validation confirmed:

* all 120 raw periods were present in every ClickHouse Gold table;
* no expected periods were missing;
* no unexpected periods were loaded;
* final row counts matched the pre-rebuild serving-layer baseline;
* the Airflow DAG completed with `success`.

This milestone validated that the pipeline can safely perform a destructive historical recalculation with explicit operator confirmation, raw-period validation, ClickHouse truncation protection, dynamic monthly processing, and month-level serving-layer quality checks.

## Completed Milestone: dbt Analytics Layer

The dbt analytical modeling layer has been implemented downstream of ClickHouse Gold.

Implemented work includes:

* configured dbt for ClickHouse;
* defined existing ClickHouse Gold tables as dbt sources;
* created staging models for normalized analytical access;
* created an intermediate transformation layer;
* created the `mart_daily_trip_kpis` analytical mart;
* added schema tests and custom data tests;
* integrated dbt execution into Airflow;
* added dbt orchestration tests;
* triggered dbt automatically after successful data-processing runs.

The current dbt analytics pipeline includes:

```text
sources
→ staging models
→ intermediate model
→ mart_daily_trip_kpis
→ dbt data tests
```

The current successful dbt build includes:

```text
7 models
5 sources
80 total checks/tests
```

The responsibility split is:

```text
Spark
→ ingestion, cleaning, large-scale transformation, monthly Gold preparation

dbt
→ downstream analytical modeling, SQL transformations, marts, tests, documentation, lineage
```

At the current stage, Superset dashboards still query the original ClickHouse Gold marts directly. Connecting selected Superset datasets to dbt marts is a planned BI-layer improvement.

## Completed Milestone: Current Serving-Layer Extension

After the validated historical full rebuild, the raw source was extended with an additional period:

```text
2026-01
```

This month was processed through:

```text
nyc_taxi_process_new_months_pipeline
```

The current validated serving-layer output covers:

```text
121 monthly periods
2016-01 → 2026-01
```

Current ClickHouse Gold row counts:

```text
gold_daily_trips                 3,684
gold_hourly_trips                265,201
gold_payment_type_stats          52,458
gold_location_pair_stats         39,815,396
```

Current dbt analytics mart coverage:

```text
mart_daily_trip_kpis
rows_count:    3,684
min_date:      2016-01-01
max_date:      2026-01-31
periods_count: 121
```

## 1. Superset and dbt BI Integration

The next BI-layer improvement is to connect selected Superset datasets and charts to dbt analytics marts where appropriate.

Planned work:

* review current Superset virtual datasets;
* identify charts that should continue using Spark Gold marts directly;
* identify charts that would benefit from dbt marts;
* connect selected Superset datasets to `nyc_taxi_analytics_dbt`;
* validate chart results before and after migration;
* update dashboard screenshots and exported Superset artifacts;
* document which BI datasets are backed by Gold marts and which are backed by dbt marts.

This should be done carefully because the current dashboard is already validated on ClickHouse Gold marts.

## 2. Expanded dbt Analytical Modeling

The initial dbt layer is implemented. Future work can expand it with additional business-facing marts.

Planned improvements:

* add payment-level dbt marts;
* add hourly-demand dbt marts;
* add route-level dbt marts;
* add airport-flow analytical models;
* add grouped-ride opportunity models;
* add model descriptions and column descriptions;
* generate dbt documentation and lineage artifacts;
* define dbt exposures for BI dashboards;
* document ownership and intended usage of each model.

## 3. Full-Range Serving-Layer Validation

The current pipelines validate every processed month independently.

A future final quality gate should validate the complete historical serving layer after all mapped monthly tasks have finished.

Planned checks:

* every expected period exists in all configured ClickHouse Gold tables;
* every expected period exists in required dbt marts;
* no periods are missing;
* no unexpected periods exist;
* minimum and maximum dates match the confirmed rebuild range;
* every table contains data;
* period counts are consistent across all Gold tables;
* Object Storage Gold periods match ClickHouse periods;
* dbt mart coverage matches ClickHouse Gold coverage;
* final row counts and processing metadata are recorded.

For the current source, the expected result is:

```text
121 monthly periods
2016-01 → 2026-01
```

The previously validated historical full rebuild remains:

```text
120 monthly periods
2016-01 → 2025-12
```

## 4. Safer Full Rebuild Publication

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
* run dbt validation against staging or a temporary analytics schema;
* perform an atomic publication step;
* preserve the previous serving-layer version;
* add rollback support;
* prevent Superset from reading incomplete rebuild data.

## 5. Resumable Full Rebuilds

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
dbt_build_status
```

## 6. ClickHouse Schema Management

Planned improvements:

* add versioned ClickHouse migrations;
* store schema changes in the repository;
* reuse `clickhouse_utils.py` as the common execution layer;
* validate compatibility between Spark Gold schemas and ClickHouse schemas;
* validate compatibility between ClickHouse Gold sources and dbt models;
* add migration checks to CI;
* document backward-incompatible changes;
* add rollback procedures where possible.

## 7. Monitoring and Observability

The current monitoring strategy includes retries, execution timeouts, structured failure callbacks, optional Telegram alerts, ClickHouse quality checks, and dbt build visibility through Airflow.

Detailed current behavior is documented in:

```text
docs/monitoring_plan.md
```

Future improvements:

* extract full historical task runtime statistics from Airflow metadata;
* recalibrate monitoring thresholds for the complete multi-year pipeline;
* add informational alerts for tasks exceeding normal runtime;
* add critical alerts before tasks reach execution timeout;
* track historical task and DAG durations;
* monitor Airflow queue time and retry frequency;
* monitor ClickHouse row counts and period completeness;
* monitor dbt mart freshness and test results;
* monitor source-data freshness;
* add Prometheus metrics;
* add Grafana dashboards;
* add Alertmanager;
* add Superset availability and query checks;
* support Slack or email notifications.

## 8. Cloud Execution

The current project runs Spark locally inside the Docker-based Airflow environment.

Future cloud work may include:

* migrate Spark workloads to Yandex Data Proc or another managed Spark runtime;
* submit remote Spark jobs from Airflow;
* poll remote job status;
* propagate cloud-job failures back to Airflow;
* separate orchestration from compute;
* use cloud Object Storage as the persistent data layer;
* evaluate managed Kafka and ClickHouse options for future projects.

Target production-like split:

```text
Airflow
→ orchestration, dependency management, retries, monitoring

Managed Spark
→ distributed data processing

Object Storage
→ persistent lake layers

ClickHouse
→ serving and analytics layer

dbt
→ SQL analytics modeling and data tests
```

## 9. Infrastructure as Code and Security

Planned infrastructure improvements:

* add Terraform;
* define cloud storage, compute, networking, and service configuration as code;
* create separate local, test, and cloud environments;
* move credentials to a managed secrets service;
* avoid long-lived credentials in local configuration;
* add role-based access controls;
* document backup and recovery procedures;
* extend CI/CD from validation to controlled deployment.

## 10. BI Reproducibility

Planned improvements:

* verify Superset dashboard import on a clean environment;
* maintain exported dashboards and datasets as versioned artifacts;
* document ClickHouse and Superset connection setup;
* document future dbt-backed Superset dataset setup;
* add automated dashboard smoke tests;
* add data-freshness indicators;
* add historical period comparison views;
* update dashboard screenshots after major BI changes;
* improve portfolio documentation.

## 11. Geospatial Analytics

Current maps use taxi-zone centroid coordinates.

Future improvements:

* load official taxi-zone polygons;
* build polygon-based choropleth maps;
* improve multi-part geometry handling;
* compare pickup and dropoff demand geographically;
* add borough-level aggregations;
* add historical demand-change maps;
* evaluate spatial indexing and geospatial query support.

## 12. Project Documentation

Current documentation includes:

```text
docs/architecture.md
docs/data_quality.md
docs/full_rebuild_runbook.md
docs/testing.md
docs/monitoring_plan.md
docs/analytics_summary.md
docs/roadmap.md
```

Planned documentation improvements:

* maintain architecture documentation as the system evolves;
* maintain testing documentation;
* document dbt models and table schemas;
* document incident and recovery procedures;
* create an interview-oriented project walkthrough;
* record future full historical runtime baselines;
* document Superset-to-dbt migration decisions;
* keep README aligned with implemented architecture.

## Priority Order

Current expected sequence:

```text
1. Connect selected Superset datasets to dbt analytics marts
2. Expand dbt analytical marts beyond mart_daily_trip_kpis
3. Add dbt documentation and lineage artifacts
4. Add an automated full-range post-rebuild quality gate
5. Add ClickHouse schema migration and versioning
6. Improve monitoring and runtime observability
7. Introduce staging tables and atomic publication
8. Add resumable full rebuild execution and recovery metadata
9. Move Spark compute to a managed cloud runtime
10. Add Terraform and production-oriented infrastructure
```
