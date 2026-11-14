# Monitoring and Observability

## Dataflow

Monitor:

- job failures
- system lag
- elapsed time
- worker CPU and memory where available
- Pub/Sub backlog
- watermark/event-time delay
- invalid-event/DLQ volume

Recommended alerts:

- streaming Dataflow job failed
- system lag above agreed threshold
- Pub/Sub backlog growing continuously
- streaming DLQ count above threshold
- batch Dataflow job exceeds normal duration

## Cloud Composer / Airflow

Monitor:

- DAG run failures
- task retries
- sensor timeouts
- Dataproc/Dataflow task duration
- reconciliation gate failures
- dbt build/test failures

The DAG intentionally prevents archival when an upstream validation or reconciliation task fails.

## BigQuery

Monitor:

- bytes processed
- slot usage
- failed jobs
- long-running queries
- partition-filter usage
- reconciliation mismatches
- audit/reject counts

Use the queries in:

`bigquery/monitoring_queries.sql`

## Dataproc / Spark

Monitor:

- job duration
- executor failures
- skewed stages
- shuffle volume
- memory pressure
- unusually large input growth

## Business Data Quality

Track per batch:

```text
source_count
stage_count
valid_count
rejected_count
duplicate_count
target_count
status
```

A pipeline can be technically successful but still fail the reconciliation gate if the data counts do not match expected processing rules.
