# Fit Analytics Portfolio Project Documentation

## Objective

Design an end-to-end GCP data platform for an apparel fit and size recommendation use case. The solution supports both historical batch processing and near-real-time recommendation events.

All records in this repository are synthetic.

## Business Entities

### Customer
Stores fit-profile attributes used for size recommendations.

### Product
Stores apparel product, brand, category, size system and product fit behaviour.

### Purchase
Stores purchased size and the recommendation shown before purchase.

### Return
Stores return reason and customer fit feedback such as RUNS_SMALL or RUNS_LARGE.

### Recommendation
Stores recommended size, confidence score and recommendation timestamp.

## Batch Architecture

```text
SFTP / NAS / API
      |
      v
GCS Landing
      |
      v
File Validation
      |
      v
Dataflow / Dataproc
      |
      v
BigQuery Stage
      |
      v
History + SCD2
      |
      v
Curated Dimensions/Facts
      |
      v
dbt Reporting Models
```

## Streaming Architecture

```text
Recommendation Event
       |
       v
    Pub/Sub
       |
       v
    Dataflow
       |
 +-----+------+
 |            |
BigQuery     DLQ
```

Streaming design includes event ID deduplication, event-time processing, windowing, watermarks and late-data handling.

## BigQuery Layers

- `fitanalytics_stage`
  - `stg_customer`
  - `stg_product`
  - `stg_purchase`
  - `stg_return`
  - `stg_recommendation`

- `fitanalytics_history`
  - historical customer/product source changes

- `fitanalytics_curated`
  - `dim_customer`
  - `dim_product`
  - `fact_purchase`
  - `fact_return`
  - `fact_recommendation`

- `fitanalytics_audit`
  - `pipeline_audit`
  - `quarantine_records`

- `fitanalytics_reporting`
  - customer fit profile
  - product performance
  - return analysis
  - recommendation performance
  - brand performance

## Data Quality Framework

### File-Level
- file existence
- file naming
- file extension
- non-zero size
- expected header

### Schema-Level
- column count
- column names
- data types
- required columns

### Row-Level
- critical NULL checks
- valid fit values
- valid size values
- positive amounts
- customer/product referential integrity
- duplicate business keys

Invalid records are written to quarantine with the original row and error reason.

## Incremental Processing

A watermark/control-table approach tracks the latest processed business timestamp. New or changed source records are loaded into stage tables and then merged to curated tables.

Reruns are idempotent by using business keys, batch IDs and MERGE logic.

## SCD Type 2

Customer and product dimensions preserve historical changes using:

- effective_start_date
- effective_end_date
- is_current
- record_hash

A SHA-256 hash is generated from business attributes to detect changes.

## Key KPIs

1. Fit-Related Return Rate by Product
2. Products that Run Small / Run Large
3. Brand Size Consistency
4. Customer Fit Profile vs Purchased Size
5. Size Recommendation Accuracy
6. Category Fit Issues
7. Regional Fit Trends
8. Financial Impact of Fit-Related Returns
