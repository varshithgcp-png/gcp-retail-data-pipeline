# Phase 2 - Run the Fit Analytics Dataflow Ingestion

## Flow

```text
GCS Landing
   |
   v
Apache Beam / Dataflow
   |
   +---- valid ------> fitanalytics_stage
   |
   +---- invalid ----> fitanalytics_audit.quarantine_records
```

The same Python pipeline is reused for customer, product, purchase, return and recommendation files through metadata configuration.

## 1. Create BigQuery Datasets and Tables

Run:

```text
bigquery/create_tables.sql
```

Replace `PROJECT_ID` first.

## 2. Suggested GCS Layout

```text
gs://YOUR_BUCKET/
├── landing/
│   ├── customer/
│   ├── product/
│   ├── purchase/
│   ├── return/
│   └── recommendation/
├── archive/
├── quarantine/
└── dataflow/
    ├── temp/
    └── staging/
```

## 3. Install Dependencies

```bash
python -m venv .venv
pip install -r requirements.txt
```

## 4. Run Unit Tests

```bash
pytest tests -v
```

## 5. Run Customer Ingestion on Dataflow

Run from the repository root:

```bash
python -m dataflow.fitanalytics_ingestion_pipeline \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --region us-central1 \
  --temp_location gs://YOUR_BUCKET/dataflow/temp \
  --staging_location gs://YOUR_BUCKET/dataflow/staging \
  --job_name fitanalytics-customer-batch \
  --entity_type customer \
  --input gs://YOUR_BUCKET/landing/customer/customer_*.csv \
  --output_table YOUR_PROJECT_ID:fitanalytics_stage.stg_customer \
  --quarantine_table YOUR_PROJECT_ID:fitanalytics_audit.quarantine_records \
  --batch_id 20261114_080000
```

## 6. Run Other Entities

Use the same command and change:

| Entity | Stage Table |
|---|---|
| product | `fitanalytics_stage.stg_product` |
| purchase | `fitanalytics_stage.stg_purchase` |
| return | `fitanalytics_stage.stg_return` |
| recommendation | `fitanalytics_stage.stg_recommendation` |

## Validation Performed

- expected column count
- required-field validation
- integer/numeric validation
- date/timestamp validation
- preferred-fit domain validation
- product fit-profile validation
- positive quantity and amount checks
- recommendation confidence between 0 and 1

Cross-table referential-integrity checks, deduplication, record hashing and SCD Type 2 are performed in the transformation phase.

## Replay Safety

Airflow supplies a `batch_id`. Stage records carry that batch ID and source-file name. Later MERGE logic uses the business key plus change detection so rerunning the same business date does not create duplicate curated records.
