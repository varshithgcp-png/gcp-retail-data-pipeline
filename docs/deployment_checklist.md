# Deployment Checklist

Use this before claiming the project has been executed end-to-end in GCP.

## BigQuery

- [ ] Create all five core datasets plus `fitanalytics_streaming`
- [ ] Run batch table DDL
- [ ] Run curated/history/control DDL
- [ ] Run streaming table DDL
- [ ] Verify partitioning and clustering
- [ ] Configure RLS/CLS where required
- [ ] Run monitoring queries

## GCS

- [ ] Create landing/archive/quarantine/temp/staging prefixes
- [ ] Enable appropriate lifecycle policies
- [ ] Upload sample batch files
- [ ] Upload Dataproc driver and `spark_jobs.zip`

## Pub/Sub

- [ ] Create recommendation topic
- [ ] Create dedicated Dataflow subscription
- [ ] Create pipeline DLQ topic
- [ ] Verify publisher message attributes

## Dataflow

- [ ] Run one batch entity locally/DirectRunner first
- [ ] Run batch Dataflow job
- [ ] Verify stage and quarantine rows
- [ ] Start streaming Dataflow job
- [ ] Publish sample streaming events
- [ ] Verify duplicate, late and invalid-event behaviour

## Dataproc

- [ ] Create or select cluster
- [ ] Verify Spark BigQuery connector
- [ ] Submit transformation driver
- [ ] Verify SCD2 history
- [ ] Verify fact MERGE results
- [ ] Verify reconciliation audit

## dbt

- [ ] Configure local/Composer profile securely
- [ ] Run `dbt debug`
- [ ] Run `dbt deps`
- [ ] Run `dbt build`
- [ ] Generate dbt docs
- [ ] Verify reporting marts/views

## Cloud Composer

- [ ] Install compatible dependencies
- [ ] Configure environment variables
- [ ] Upload DAG
- [ ] Trigger manual test run
- [ ] Test explicit backfill with business date + batch ID
- [ ] Confirm files archive only after success

## CI/CD

- [ ] Configure Workload Identity Pool/Provider
- [ ] Configure GitHub WIF provider secret
- [ ] Configure deployment service account
- [ ] Configure repository variables
- [ ] Run CI workflow
- [ ] Run manual deploy workflow

## Portfolio Evidence

Capture screenshots only after the environment is genuinely running:

- [ ] Composer DAG graph
- [ ] successful Dataflow batch job
- [ ] running streaming Dataflow graph
- [ ] Pub/Sub topic/subscription
- [ ] BigQuery datasets/tables
- [ ] SCD2 customer example
- [ ] dbt lineage/docs
- [ ] reconciliation query output
- [ ] Fit KPI query output

Do not present screenshots as production/client evidence. Label them as portfolio/synthetic GCP execution.
