# Protected Full Rebuild Runbook

This document describes how to safely run, monitor, validate, and recover the protected NYC Taxi full rebuild pipeline.

DAG:

```text
nyc_taxi_full_rebuild_pipeline
```

The full rebuild is destructive because it truncates all configured ClickHouse Gold tables before reloading validated raw periods.

After a successful full rebuild, Airflow triggers the downstream dbt analytics pipeline:

```text
nyc_taxi_dbt_analytics_pipeline
```

This rebuilds and validates dbt analytics models from the refreshed ClickHouse Gold source tables.

## Current Source Range

The current Object Storage source contains:

```text
121 consecutive monthly periods
2016-01 → 2026-01
```

The confirmed expected range must exactly match the periods discovered at runtime.

The current serving layer contains `2026-01`, which was added after the initial validated historical full rebuild through the new-month processing pipeline.

## Safety Model

The DAG cannot reach ClickHouse truncation unless the following chain succeeds:

```text
create ClickHouse Gold tables
        │
        ▼
validate explicit runtime confirmations
        │
        ▼
discover raw Yellow Taxi periods
        │
        ▼
validate discovered periods against the expected range
        │
        ▼
log the rebuild plan
        │
        ▼
truncate ClickHouse Gold tables
```

The run is rejected when:

* the rebuild mode is missing or incorrect;
* full rebuild confirmation is missing;
* ClickHouse truncation confirmation is missing;
* expected period values are missing or invalid;
* the expected range is reversed;
* no raw periods are found;
* expected periods are missing;
* unexpected periods exist outside the confirmed range.

## Preflight Checks

Before triggering the DAG, confirm that no related pipeline is running:

```bash
docker compose exec airflow airflow dags list-runs \
  -d nyc_taxi_full_rebuild_pipeline \
  --state running

docker compose exec airflow airflow dags list-runs \
  -d nyc_taxi_period_refresh_pipeline \
  --state running

docker compose exec airflow airflow dags list-runs \
  -d nyc_taxi_process_new_months_pipeline \
  --state running

docker compose exec airflow airflow dags list-runs \
  -d nyc_taxi_dbt_analytics_pipeline \
  --state running
```

All commands should return:

```text
No data found
```

Check disk space:

```bash
docker compose exec airflow df -h /tmp /opt/airflow
docker compose exec clickhouse df -h /var/lib/clickhouse
```

Check the Spark pool:

```bash
docker compose exec airflow airflow pools get spark_pool
```

Expected local configuration:

```text
pool: spark_pool
slots: 1
```

Check Docker services:

```bash
docker compose ps
```

Keep Docker and the host machine running for the entire rebuild.

On macOS, sleep can be prevented with:

```bash
caffeinate -dimsu
```

## Raw Source Verification

Run a read-only source check before triggering the rebuild:

```bash
docker compose exec airflow bash -lc \
"cd /opt/airflow && PYTHONPATH=/opt/airflow/jobs python - <<'PY'
from raw_discovery import list_raw_yellow_periods

periods = list_raw_yellow_periods()

print(f'Raw period count: {len(periods)}')

if periods:
    print(f'First raw period: {periods[0][0]}-{periods[0][1]}')
    print(f'Last raw period: {periods[-1][0]}-{periods[-1][1]}')
PY"
```

Expected result for the current source:

```text
Raw period count: 121
First raw period: 2016-01
Last raw period: 2026-01
```

## Capture ClickHouse Baseline

Before the rebuild, store current row counts:

```bash
set -a
source .env
set +a

docker compose exec clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --database "$CLICKHOUSE_DATABASE" \
  --query "
    SELECT
        table,
        sum(rows) AS rows
    FROM system.parts
    WHERE active
      AND database = currentDatabase()
      AND table IN (
          'gold_daily_trips',
          'gold_hourly_trips',
          'gold_location_pair_stats',
          'gold_payment_type_stats'
      )
    GROUP BY table
    ORDER BY table
  " > /tmp/full_rebuild_before.tsv

cat /tmp/full_rebuild_before.tsv
```

The baseline is useful for negative safety tests and operational comparison.

For the current serving layer, the expected baseline after `2026-01` processing is:

```text
gold_daily_trips                 3,684
gold_hourly_trips                265,201
gold_payment_type_stats          52,458
gold_location_pair_stats         39,815,396
```

## Trigger Configuration

Required Airflow Trigger DAG config for the current raw source:

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

Trigger from the command line:

```bash
RUN_ID="manual__positive_full_raw_rebuild_$(date -u +%Y%m%dT%H%M%SZ)"

docker compose exec airflow airflow dags trigger \
  --run-id "$RUN_ID" \
  --conf '{
    "rebuild_mode": "full_raw_rebuild",
    "confirm_full_rebuild": true,
    "confirm_clickhouse_truncate": true,
    "expected_start_year": "2016",
    "expected_start_month": "01",
    "expected_end_year": "2026",
    "expected_end_month": "01"
  }' \
  nyc_taxi_full_rebuild_pipeline

echo "$RUN_ID"
```

## Expected Initial Task Flow

Before monthly processing begins, the following tasks should succeed:

```text
create_clickhouse_gold_tables
validate_full_rebuild_config
discover_full_rebuild_raw_periods
validate_full_rebuild_raw_periods
log_full_rebuild_plan
truncate_clickhouse_gold_tables
```

After truncation, Airflow dynamically maps the monthly pipeline across all validated periods.

## Monitoring the Run

Check the DAG run:

```bash
docker compose exec airflow airflow dags list-runs \
  -d nyc_taxi_full_rebuild_pipeline
```

Check task states:

```bash
docker compose exec airflow airflow tasks states-for-dag-run \
  nyc_taxi_full_rebuild_pipeline \
  "$RUN_ID"
```

Each monthly mapped group runs:

```text
Bronze
→ Silver
→ Silver quality
→ four Gold marts
→ Gold quality
→ four ClickHouse loads
→ ClickHouse month quality
```

After all monthly processing succeeds, the DAG triggers the dbt analytics pipeline:

```text
nyc_taxi_dbt_analytics_pipeline
```

The dbt child DAG runs:

```text
dbt debug
→ dbt build
```

The local configuration keeps tasks sequential:

```text
max_active_runs = 1
max_active_tasks = 1
spark_pool slots = 1
```

## Failure and Retry Behavior

Tasks use:

```text
retries = 1
retry delay = 5 minutes
```

After the first failure, a task may remain in:

```text
up_for_retry
```

for approximately five to six minutes.

After the second failure, it becomes:

```text
failed
```

Downstream tasks are then marked:

```text
upstream_failed
```

The shared Airflow callback writes structured failure details to task logs and can send an optional Telegram alert.

If the dbt child DAG fails, the dbt trigger task in the parent full rebuild DAG fails when it waits for completion.

## Important Availability Limitation

The current implementation truncates active ClickHouse Gold tables before monthly loading begins.

During the rebuild:

* ClickHouse is initially empty;
* periods are restored gradually;
* Superset may show incomplete data;
* a failed run may leave the serving layer partially loaded.

Do not trigger period refresh, new-month processing, or manual dbt rebuild while the full rebuild is active.

## Recovery After a Monthly Failure

Before retrying or clearing tasks:

1. Inspect the failed task log.
2. Identify whether the failure is caused by code, data, infrastructure, or timeout.
3. Confirm which mapped period failed.
4. Check whether that period is partially loaded into ClickHouse.
5. Fix the underlying issue.
6. Clear only the failed task and its required downstream tasks when safe.

Do not restart the entire full rebuild automatically without checking the current ClickHouse state.

The current implementation does not yet provide atomic publication or automatic rollback.

## Recovery After a dbt Failure

If the Spark and ClickHouse part of the full rebuild succeeds but the downstream dbt analytics DAG fails:

1. Inspect the failed `dbt_debug` or `dbt_build_analytics_layer` task log.
2. Confirm whether the failure is caused by ClickHouse connectivity, dbt model logic, dbt tests, or runtime configuration.
3. Fix the underlying issue.
4. Re-run the dbt analytics pipeline after the issue is fixed.

Manual dbt validation through Airflow:

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_debug 2026-01-03"
```

```bash
docker compose exec airflow bash -lc \
"airflow tasks test nyc_taxi_dbt_analytics_pipeline dbt_build_analytics_layer 2026-01-03"
```

## Final Validation

After the DAG finishes successfully, confirm that every expected period exists in all four Gold tables.

Check the date range:

```bash
docker compose exec -T clickhouse bash -lc '
clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT
        min(pickup_date) AS min_pickup_date,
        max(pickup_date) AS max_pickup_date
    FROM nyc_taxi.gold_daily_trips
    FORMAT Vertical
  "
'
```

Expected result for the current source:

```text
min_pickup_date: 2016-01-01
max_pickup_date: 2026-01-31
```

Check period counts and row counts:

```bash
docker compose exec -T clickhouse bash -lc '
clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT
        table_name,
        rows_count,
        min_date,
        max_date,
        periods_count
    FROM
    (
        SELECT
            '\''gold_daily_trips'\'' AS table_name,
            count() AS rows_count,
            min(pickup_date) AS min_date,
            max(pickup_date) AS max_date,
            uniqExact(toYYYYMM(pickup_date)) AS periods_count
        FROM nyc_taxi.gold_daily_trips

        UNION ALL

        SELECT
            '\''gold_hourly_trips'\'' AS table_name,
            count() AS rows_count,
            min(pickup_date) AS min_date,
            max(pickup_date) AS max_date,
            uniqExact(toYYYYMM(pickup_date)) AS periods_count
        FROM nyc_taxi.gold_hourly_trips

        UNION ALL

        SELECT
            '\''gold_payment_type_stats'\'' AS table_name,
            count() AS rows_count,
            min(pickup_date) AS min_date,
            max(pickup_date) AS max_date,
            uniqExact(toYYYYMM(pickup_date)) AS periods_count
        FROM nyc_taxi.gold_payment_type_stats

        UNION ALL

        SELECT
            '\''gold_location_pair_stats'\'' AS table_name,
            count() AS rows_count,
            min(pickup_date) AS min_date,
            max(pickup_date) AS max_date,
            uniqExact(toYYYYMM(pickup_date)) AS periods_count
        FROM nyc_taxi.gold_location_pair_stats
    )
    ORDER BY table_name
    FORMAT PrettyCompact
  "
'
```

Expected result for the current source:

```text
121 periods in every Gold table
```

Validate the dbt analytics mart:

```bash
docker compose exec -T clickhouse bash -lc '
clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT
        count() AS rows_count,
        min(pickup_date) AS min_date,
        max(pickup_date) AS max_date,
        uniqExact(toYYYYMM(pickup_date)) AS periods_count
    FROM nyc_taxi_analytics_dbt.mart_daily_trip_kpis
    FORMAT Vertical
  "
'
```

Expected dbt mart result for the current source:

```text
rows_count:    3,684
min_date:      2016-01-01
max_date:      2026-01-31
periods_count: 121
```

Record:

* DAG start and end time;
* total runtime;
* final row counts;
* period count per table;
* dbt build result;
* failures and retries;
* operational observations.

## Validated Negative Scenarios

### Missing Confirmation

A run without explicit confirmation was rejected:

```text
validate_full_rebuild_config → failed
truncate_clickhouse_gold_tables → upstream_failed
```

ClickHouse row counts remained unchanged.

### Mismatched Expected Range

A run confirmed only for 2024 while Object Storage contained `2016-01 → 2025-12` was rejected:

```text
validate_full_rebuild_config → success
discover_full_rebuild_raw_periods → success
validate_full_rebuild_raw_periods → failed
truncate_clickhouse_gold_tables → upstream_failed
```

ClickHouse row counts again remained unchanged.

These negative scenarios validated that destructive truncation is protected by explicit runtime confirmation and raw-period range validation.

## Validated Positive Full Rebuild

The protected full rebuild was successfully executed against the complete historical raw source available at that time.

Airflow run details:

```text
DAG ID:      nyc_taxi_full_rebuild_pipeline
Run ID:      manual__positive_full_raw_rebuild_20260614T123022Z
State:       success
Start time:  2026-06-14 12:30:24 UTC
End time:    2026-06-15 12:13:02 UTC
Duration:    23 h 42 min 37 sec
```

Validated source range:

```text
raw period count:                     120
fully processed ClickHouse periods:   120
date range:                           2016-01 → 2025-12
missing periods:                      none
unexpected periods:                   none
```

Final ClickHouse row counts:

```text
gold_daily_trips                 3,653
gold_hourly_trips                262,969
gold_payment_type_stats          51,993
gold_location_pair_stats         39,368,728
```

The final row counts matched the serving-layer baseline captured before the rebuild.

This confirms that the pipeline reproduced the complete historical ClickHouse output after:

```text
explicit runtime confirmation
→ raw source discovery
→ raw range validation
→ ClickHouse truncation
→ processing of 120 monthly periods
→ month-level serving-layer validation
```

After this historical full rebuild, the additional `2026-01` raw period was processed through the new-month pipeline and included in the current serving-layer output.

## Validated Current Serving-Layer Output

The current validated serving-layer output covers:

```text
121 periods
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

## Operational Observation

The first attempt of `truncate_clickhouse_gold_tables` failed before executing any ClickHouse query because the script referenced an outdated variable name:

```text
GOLD_TABLES
```

The script already imported the shared configuration constant:

```text
GOLD_CLICKHOUSE_TABLES
```

The reference was corrected, a dedicated regression test was added, and the Airflow retry completed successfully.

Because the `NameError` occurred before entering the truncation loop, no table was partially truncated during the failed attempt.

The completed run therefore also validated:

* Airflow retry behavior;
* code correction through the mounted project volume;
* continued processing after the successful retry;
* full serving-layer reconstruction after a destructive rebuild;
* reproducibility of final period coverage and row counts.

## Planned Reliability Improvements

Future improvements include:

* staging ClickHouse tables;
* complete staging validation;
* atomic table swap;
* rollback to the previous serving-layer version;
* resumable rebuild execution;
* automated full-range final quality validation across ClickHouse Gold and dbt marts;
* persistent rebuild metadata;
* dbt documentation and lineage artifacts.