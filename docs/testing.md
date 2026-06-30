# Testing Strategy

This document describes the automated testing strategy for the NYC Taxi data engineering pipeline.

The project contains more than 300 tests covering:

* configuration and runtime parameters;
* Spark transformations;
* Airflow DAG structure and task dependencies;
* dynamic task mapping;
* raw period discovery;
* protected full rebuild safety;
* dbt analytics orchestration;
* ClickHouse utilities and query construction;
* data quality rules;
* failure callbacks and Telegram alerting.

The automated test suite focuses on deterministic unit, transformation, utility, and DAG-structure checks.

Runtime Spark execution, ClickHouse loads, and dbt builds are validated locally through Docker Compose and Airflow.

## Test Categories

### Configuration and Period Validation

Covered modules include:

```text
tests/test_config.py
tests/test_period_utils.py
tests/test_period_refresh_config.py
tests/test_full_rebuild_config.py
```

These tests validate:

* environment-driven configuration;
* Object Storage paths;
* ClickHouse settings;
* dbt-related runtime configuration where applicable;
* Airflow retry and timeout defaults;
* year/month normalization;
* same-year and cross-year period generation;
* invalid and reversed period ranges;
* period refresh runtime configuration;
* protected full rebuild confirmations;
* non-destructive full rebuild defaults;
* safety messages confirming that ClickHouse truncation did not run after failed validation.

### Raw Period Discovery

Covered by:

```text
tests/test_raw_discovery.py
```

The tests validate:

* raw Yellow Taxi object key parsing;
* path and filename period matching;
* invalid layer and taxi-type filtering;
* duplicate removal;
* chronological sorting;
* expected period generation;
* missing and unexpected period detection;
* exact matching between discovered and confirmed full rebuild periods;
* raw periods minus fully processed ClickHouse periods;
* partially loaded month detection;
* intersection across all configured Gold tables.

Object Storage and ClickHouse responses are mocked.

A month is considered fully processed only when it exists in all configured ClickHouse Gold tables:

```text
gold_daily_trips
gold_hourly_trips
gold_payment_type_stats
gold_location_pair_stats
```

This protects the new-month pipeline from skipping partially loaded periods.

### Airflow DAG Structure

Covered by:

```text
tests/test_full_rebuild_dag.py
tests/test_period_refresh_dag.py
tests/test_process_new_months_dag.py
tests/test_dbt_analytics_dag.py
```

The tests validate:

* DAG imports;
* DAG IDs;
* safe runtime parameters;
* top-level tasks;
* mapped `process_month` TaskGroups;
* task dependency order;
* dynamic task mapping;
* absence of outdated static month-specific tasks;
* Spark pool usage;
* retries and retry delays;
* task-family-specific execution timeouts;
* shared failure callbacks;
* separation between Spark and non-Spark tasks;
* dbt analytics DAG structure;
* dbt trigger behavior from upstream processing DAGs.

#### Protected Full Rebuild DAG

The tests verify the complete pre-truncation chain:

```text
create ClickHouse tables
→ validate runtime confirmations
→ discover raw periods
→ validate raw periods
→ log rebuild plan
→ truncate ClickHouse
→ process mapped months
→ trigger dbt analytics
```

The truncation task cannot run when configuration or raw source validation fails.

After successful monthly processing, the DAG triggers the dbt analytics pipeline.

#### Period Refresh DAG

The tests verify:

```text
read selected interval
→ delete selected month
→ rebuild monthly layers
→ reload ClickHouse
→ validate the month
→ trigger dbt analytics
```

This confirms that corrected periods are followed by a downstream analytics rebuild.

#### New-Month DAG

The tests verify:

```text
discover unprocessed periods
→ clean partial month data
→ process mapped months
→ validate each loaded month
→ trigger dbt analytics if any period was processed
```

The no-new-period case is also supported as a successful no-op.

In the no-op branch, the DAG skips the dbt trigger because no ClickHouse Gold data changed.

#### dbt Analytics DAG

The tests verify:

```text
dbt debug
→ dbt build
```

They also validate that the dbt DAG is configured as a separate downstream analytics pipeline rather than being embedded inside each monthly Spark TaskGroup.

### Spark Transformation Tests

Covered modules include:

```text
tests/test_silver_transformations.py
tests/test_gold_daily_transformations.py
tests/test_gold_hourly_transformations.py
tests/test_gold_payment_type_transformations.py
tests/test_gold_location_pair_transformations.py
```

The tests use small in-memory Spark DataFrames.

They validate:

* Silver data quality flags;
* bad-record conditions;
* derived pickup date and hour;
* trip duration;
* trip type classification;
* daily aggregations;
* hourly aggregations;
* payment type mapping and metrics;
* route-level aggregations;
* taxi zone enrichment.

### Data Quality Tests

Covered modules include:

```text
tests/test_check_yellow_taxi_quality.py
tests/test_check_gold_schema.py
tests/test_check_clickhouse_gold_quality.py
tests/test_check_clickhouse_gold_month_quality.py
```

The tests validate:

* Silver row counts and row-loss thresholds;
* required fields;
* payment type and location domains;
* pickup-hour boundaries;
* Gold schemas;
* monthly date boundaries;
* ClickHouse table existence;
* monthly ClickHouse metrics;
* route and payment metadata;
* serving-layer quality failures.

Detailed validation rules are documented in:

```text
docs/data_quality.md
```

### dbt Analytics Validation

dbt model execution and data tests are validated through the local Airflow/dbt runtime.

The dbt analytics pipeline is:

```text
nyc_taxi_dbt_analytics_pipeline
```

It runs:

```text
dbt debug
→ dbt build
```

The current successful dbt build includes:

```text
7 models
5 sources
80 total checks/tests
```

dbt validates downstream analytical assumptions such as:

* required fields are not null;
* accepted values are respected;
* analytical grain is unique;
* downstream mart logic is internally consistent;
* duplicated analytical grains are detected.

During implementation, dbt tests detected duplicated analytical grains in the historical `2021-09` period. The issue was fixed through the safe period refresh pipeline, after which the dbt build passed again.

### ClickHouse Utility and Cleanup Tests

Covered modules include:

```text
tests/test_clickhouse_utils.py
tests/test_truncate_clickhouse_gold_tables.py
tests/test_delete_clickhouse_gold_month.py
```

The tests validate:

* ClickHouse HTTP URL construction;
* Basic Auth;
* query execution;
* HTTP and connection error handling;
* JSON parsing;
* `TRUNCATE TABLE IF EXISTS` query construction;
* iteration across `GOLD_CLICKHOUSE_TABLES`;
* monthly `ALTER TABLE ... DELETE` query construction;
* asynchronous mutation polling.

Potentially destructive calls are mocked.

Unit tests do not execute real:

```text
TRUNCATE TABLE
ALTER TABLE ... DELETE
```

queries against the project database.

### Airflow Failure Callback Tests

Covered by:

```text
tests/test_airflow_callbacks.py
```

The tests validate:

* safe extraction of Airflow context values;
* structured failure message generation;
* missing-context fallback behavior;
* Airflow log output;
* Telegram configuration checks;
* mocked Telegram API calls;
* safe handling of Telegram delivery errors.

## Runtime Safety Validation

Automated tests are complemented by real Airflow negative and positive validation runs.

### Missing Confirmation

The full rebuild DAG was triggered without runtime confirmation:

```text
validate_full_rebuild_config → failed
truncate_clickhouse_gold_tables → upstream_failed
```

ClickHouse row counts remained unchanged.

### Mismatched Raw Range

The DAG was confirmed for 2024 while Object Storage contained periods from 2016 through 2025:

```text
validate_full_rebuild_config → success
discover_full_rebuild_raw_periods → success
validate_full_rebuild_raw_periods → failed
truncate_clickhouse_gold_tables → upstream_failed
```

ClickHouse row counts again remained unchanged.

These tests confirm that destructive operations are blocked by real Airflow task dependencies, not only by isolated unit-test logic.

## Runtime End-to-End Validation

### Validated Historical Full Rebuild

The protected full rebuild was successfully executed against the full raw source available at that time:

```text
120 monthly periods
2016-01 → 2025-12
runtime: 23 h 42 min 37 sec
```

The run confirmed:

* explicit runtime confirmation worked;
* raw period discovery worked;
* raw range validation worked;
* ClickHouse truncation was protected by upstream checks;
* all selected periods were processed through mapped monthly TaskGroups;
* each month passed ClickHouse month-level quality validation;
* final row counts matched the captured baseline.

### New-Month Processing

The `2026-01` raw period was added later and processed through:

```text
nyc_taxi_process_new_months_pipeline
```

Runtime validation confirmed:

```text
new raw period 2026-01
→ discovered by new-month processing
→ processed through Spark and ClickHouse
→ triggered dbt analytics pipeline
→ dbt build passed
```

### Period Refresh with dbt Validation

A historical duplicate-grain issue was detected by dbt tests in the `2021-09` period.

Runtime validation confirmed:

```text
duplicated analytical grains in 2021-09
→ detected by dbt tests
→ fixed through nyc_taxi_period_refresh_pipeline
→ dbt analytics pipeline triggered automatically
→ dbt build passed after refresh
```

This validated the full correction path from analytics-layer failure detection to safe monthly period replacement.

## Running Tests

Run the full test suite locally:

```bash
PYTHONPATH=jobs python -m pytest tests -v
```

Run tests inside the active Airflow container:

```bash
docker compose exec airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest tests -v"
```

Run tests without starting the main Airflow scheduler:

```bash
docker compose run --rm airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest tests -v"
```

Run protected full rebuild tests:

```bash
docker compose exec airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest \
tests/test_full_rebuild_config.py \
tests/test_raw_discovery.py \
tests/test_full_rebuild_dag.py \
tests/test_truncate_clickhouse_gold_tables.py \
-v"
```

Run Airflow DAG tests:

```bash
docker compose exec airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs pytest \
tests/test_full_rebuild_dag.py \
tests/test_period_refresh_dag.py \
tests/test_process_new_months_dag.py \
tests/test_dbt_analytics_dag.py \
-v"
```

Run dbt analytics task checks through Airflow:

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_debug 2026-01-03"
```

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_build_analytics_layer 2026-01-03"
```

## GitHub Actions CI

The workflow is defined in:

```text
.github/workflows/ci.yml
```

CI runs:

* Python syntax checks;
* the complete pytest suite;
* Airflow DAG import and dependency tests;
* Spark transformation tests;
* data quality tests;
* full rebuild safety tests;
* dbt orchestration structure tests;
* ClickHouse utility and cleanup tests;
* callback and Telegram tests with mocked external calls.

Most tests do not require live Object Storage, ClickHouse, dbt runtime, or Telegram access because external calls are mocked or validated through structure-level tests.

Runtime Spark execution, ClickHouse loads, and dbt builds are validated locally through Docker Compose and Airflow rather than in GitHub Actions CI.