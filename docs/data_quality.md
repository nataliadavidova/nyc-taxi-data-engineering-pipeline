# Data Quality Strategy

This document describes the data quality gates used by the NYC Taxi data engineering pipeline.

The pipeline validates data at four main stages:

```text id="4fptiq"
Silver transformation
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

A monthly processing period is considered successfully loaded into the serving layer only after all applicable Spark, Object Storage, and ClickHouse quality gates have passed.

The downstream analytics layer is considered ready only after the dbt analytics build and its data tests pass.

## Silver Data Quality

Silver validation is implemented in:

```text id="ehjw69"
jobs/silver_yellow_taxi.py
jobs/check_yellow_taxi_quality.py
```

The Silver layer validates source records before they are used to build analytical marts.

Main checks include:

* pickup and dropoff timestamps are present;
* dropoff time is later than pickup time;
* pickup date belongs to the expected processing month;
* trip duration is positive and within the accepted range;
* trip distance is positive and not an extreme outlier;
* fare and total amounts are not negative;
* passenger count is valid;
* payment type belongs to the supported domain;
* pickup and dropoff location identifiers are populated and positive;
* derived pickup hour is between `0` and `23`;
* required analytical fields are not null.

Invalid records are excluded from the valid Silver dataset and written to the bad-records layer.

The Silver quality job also validates:

* source and output row counts;
* acceptable row-loss levels;
* required columns;
* month boundaries;
* supported domain values.

## Gold Object Storage Quality

Gold validation is implemented in:

```text id="fh735q"
jobs/check_gold_schema.py
```

The job validates the monthly analytical marts before they are loaded into ClickHouse.

It checks that:

* all expected Gold Parquet datasets can be read;
* all required columns are present;
* every Gold mart contains data;
* pickup dates belong to the selected processing month;
* trip type is populated where required;
* pickup hour values are valid;
* payment type names are populated;
* pickup and dropoff zone names are populated.

The validated Gold marts are:

```text id="jxh8wy"
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

A ClickHouse load cannot start when the Gold Object Storage quality gate fails.

## ClickHouse Month-Level Quality

Month-level serving-layer validation is implemented in:

```text id="xj5sg7"
jobs/check_clickhouse_gold_month_quality.py
```

It is the main operational ClickHouse quality gate used by all three Airflow data-processing scenarios:

```text id="vl43de"
nyc_taxi_full_rebuild_pipeline
nyc_taxi_period_refresh_pipeline
nyc_taxi_process_new_months_pipeline
```

For one selected year and month, it validates that:

* all configured ClickHouse Gold tables exist;
* every table contains rows for the selected period;
* pickup dates belong to the expected calendar month;
* trip counts are positive;
* trip type is populated in hourly, payment, and route marts;
* pickup hour is between `0` and `23`;
* pickup and dropoff zone names are populated;
* payment type names are populated.

For example, the expected date range for May 2024 is:

```text id="8kq95z"
2024-05-01 <= pickup_date < 2024-06-01
```

A period is treated as successfully processed only after this check passes.

## Full Serving-Layer Validation

A broader standalone ClickHouse validation job is available in:

```text id="ctvni0"
jobs/check_clickhouse_gold_quality.py
```

It validates common serving-layer properties across the configured Gold tables, including:

* table existence;
* non-empty tables;
* valid analytical dimensions;
* populated route metadata;
* populated payment metadata.

The dynamically mapped Airflow pipelines primarily use month-level validation because their processing unit is one monthly period.

A final full-range validation gate across all expected periods is planned as a future improvement.

## dbt Analytics Quality

The dbt analytics layer validates downstream analytical models after ClickHouse Gold has been updated.

The dbt project is stored in:

```text id="ux3s2m"
dbt/
```

It reads validated ClickHouse Gold tables as sources and builds analytics models in:

```text id="9hukgm"
nyc_taxi_analytics_dbt
```

The dbt analytics pipeline is orchestrated by Airflow through:

```text id="0oe6mu"
nyc_taxi_dbt_analytics_pipeline
```

The DAG runs:

```text id="p4mse8"
dbt debug
        │
        ▼
dbt build
```

`dbt debug` validates the dbt runtime environment, project configuration, profile configuration, and ClickHouse connectivity.

`dbt build` runs dbt models and data tests.

The current dbt layer includes:

```text id="s9vo3e"
sources
→ staging models
→ intermediate model
→ mart_daily_trip_kpis
→ dbt data tests
```

The current successful dbt build includes:

```text id="e8n444"
7 models
5 sources
80 total checks/tests
```

dbt tests validate analytical assumptions such as:

* required fields are not null;
* model grains are unique;
* accepted values are respected;
* source-to-model relationships remain consistent;
* downstream mart logic is internally consistent;
* duplicated analytical grains are detected.

During implementation, dbt tests detected duplicated analytical grains in the historical `2021-09` period. The issue was fixed through the safe period refresh pipeline, after which the dbt analytics build passed again.

## Quality Gates by Pipeline

### Protected Full Rebuild

```text id="7oovt3"
Bronze
→ Silver
→ Silver quality
→ Gold marts
→ Gold Object Storage quality
→ ClickHouse loads
→ ClickHouse month quality
→ dbt analytics build
```

Every validated raw period receives an independent month-level ClickHouse quality check.

After all selected periods are processed successfully, Airflow triggers the dbt analytics pipeline to rebuild and validate downstream analytical models.

### Period Refresh

```text id="oq79s3"
delete selected ClickHouse month
→ rebuild monthly layers
→ reload Gold marts
→ validate the selected month
→ dbt analytics build
```

The quality gate confirms that the replaced period is complete after reloading.

After a successful refresh, Airflow triggers the dbt analytics pipeline so downstream models are rebuilt from corrected ClickHouse Gold data.

### New-Month Processing

```text id="q2zqj8"
discover new or partial period
→ clean existing partial ClickHouse data
→ process monthly pipeline
→ validate the loaded month
→ dbt analytics build if any period was processed
```

A month is considered fully processed only if it is present in all four Gold tables and passes month-level validation.

If no new or incomplete raw periods are discovered, the DAG completes successfully as a no-op and skips the dbt trigger.

### dbt Analytics Pipeline

```text id="yblyyi"
validate dbt runtime and ClickHouse connection
→ build analytics models
→ run dbt data tests
```

The dbt pipeline does not replace the Spark, Gold, or ClickHouse quality gates. It adds a downstream analytics-quality layer on top of validated ClickHouse Gold data.

## Failure Behavior

When a Spark, Object Storage, ClickHouse, or dbt quality check fails:

* the Airflow task fails;
* downstream tasks do not run;
* one retry is attempted after the configured retry delay;
* the shared failure callback writes a structured error message to Airflow logs;
* an optional Telegram alert is sent when alerting is enabled.

Quality failures therefore prevent incomplete or invalid data from being treated as successfully processed.

For dbt failures, `dbt debug` or `dbt build` fails inside the dbt analytics DAG. When a parent data-processing DAG waits for the dbt child DAG, the parent run does not complete successfully if the dbt validation fails.

## Automated Tests

Quality logic is covered by tests including:

```text id="gbrcdc"
tests/test_check_yellow_taxi_quality.py
tests/test_check_gold_schema.py
tests/test_check_clickhouse_gold_quality.py
tests/test_check_clickhouse_gold_month_quality.py
tests/test_silver_transformations.py
tests/test_gold_daily_transformations.py
tests/test_gold_hourly_transformations.py
tests/test_gold_payment_type_transformations.py
tests/test_gold_location_pair_transformations.py
```

Airflow and dbt orchestration behavior is covered by DAG-structure tests, including validation that:

* quality gates are part of task dependencies;
* destructive operations are protected by upstream safety checks;
* monthly processing is dynamically mapped;
* data-processing DAGs trigger dbt analytics after successful processing;
* new-month processing skips the dbt trigger when there are no eligible periods;
* the dbt analytics DAG runs `dbt debug` before `dbt build`.

The tests use small in-memory Spark DataFrames and mocked ClickHouse responses where possible, so most quality logic can be validated without real Object Storage or ClickHouse infrastructure.

Runtime Spark execution, ClickHouse loads, and dbt builds are validated locally through Docker Compose and Airflow.