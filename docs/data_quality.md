# Data Quality Strategy

This document describes the data quality gates used by the NYC Taxi data engineering pipeline.

The pipeline validates data at three main stages:

```text
Silver transformation
        │
        ▼
Gold Object Storage
        │
        ▼
ClickHouse serving layer
```

A processing period is considered successfully completed only after all applicable quality gates have passed.

## Silver Data Quality

Silver validation is implemented in:

```text
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

```text
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

```text
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

A ClickHouse load cannot start when the Gold Object Storage quality gate fails.

## ClickHouse Month-Level Quality

Month-level serving-layer validation is implemented in:

```text
jobs/check_clickhouse_gold_month_quality.py
```

It is the main operational ClickHouse quality gate used by all three Airflow processing scenarios:

```text
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

```text
2024-05-01 <= pickup_date < 2024-06-01
```

A period is treated as successfully processed only after this check passes.

## Full Serving-Layer Validation

A broader standalone ClickHouse validation job is available in:

```text
jobs/check_clickhouse_gold_quality.py
```

It validates common serving-layer properties across the configured Gold tables, including:

* table existence;
* non-empty tables;
* valid analytical dimensions;
* populated route metadata;
* populated payment metadata.

The dynamically mapped Airflow pipelines primarily use month-level validation because their processing unit is one monthly period.

## Quality Gates by Pipeline

### Protected Full Rebuild

```text
Bronze
→ Silver
→ Silver quality
→ Gold marts
→ Gold Object Storage quality
→ ClickHouse loads
→ ClickHouse month quality
```

Every validated raw period receives an independent month-level ClickHouse quality check.

### Period Refresh

```text
delete selected ClickHouse month
→ rebuild monthly layers
→ reload Gold marts
→ validate the selected month
```

The quality gate confirms that the replaced period is complete after reloading.

### New-Month Processing

```text
discover new or partial period
→ clean existing partial ClickHouse data
→ process monthly pipeline
→ validate the loaded month
```

A month is considered fully processed only if it is present in all four Gold tables and passes month-level validation.

## Failure Behavior

When a quality check fails:

* the Airflow task fails;
* downstream tasks do not run;
* one retry is attempted after the configured retry delay;
* the shared failure callback writes a structured error message to Airflow logs;
* an optional Telegram alert is sent when alerting is enabled.

Quality failures therefore prevent incomplete or invalid data from being treated as successfully processed.

## Automated Tests

Quality logic is covered by tests including:

```text
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

The tests use small in-memory Spark DataFrames and mocked ClickHouse responses where possible, so most quality logic can be validated without real Object Storage or ClickHouse infrastructure.
