# Phase 4 - dbt on BigQuery

## Purpose

Phase 4 converts the curated Fit Analytics warehouse into tested reporting marts and business-facing views.

```text
fitanalytics_curated
       |
       v
 dbt staging views
       |
       v
 dbt KPI marts
       |
       v
fitanalytics_reporting
       |
       v
Reporting Views / BI
```

## Models

### Staging

- `stg_dim_customer`
- `stg_dim_product`
- `stg_fact_purchase`
- `stg_fact_return`
- `stg_fact_recommendation`

### KPI Marts

- `mart_product_fit_performance`
- `mart_recommendation_accuracy`
- `mart_brand_size_consistency`
- `mart_region_fit_trends`

### Reporting Views

- `vw_customer_fit_profile`
- `vw_product_performance`
- `vw_return_analysis`
- `vw_recommendation_performance`
- `vw_brand_performance`

## Main KPIs

- Fit-Related Return Rate
- Recommendation Accuracy
- Recommendation Acceptance Rate
- Runs Small / Runs Large Share
- True-to-Size Share
- Fit-Related Return Value
- Regional Fit Trends

## Incremental Strategy

`mart_product_fit_performance` is an incremental BigQuery model using `merge`.

Business key:

```text
metric_date + product_id
```

A 7-day lookback is intentionally reprocessed during incremental runs. This allows recent partitions to be corrected when returns arrive after the original purchase.

## Setup

From the repository root:

```bash
python -m venv .venv
pip install -r requirements.txt
cd dbt
dbt deps
```

Copy the profile example to your local dbt profiles directory and set:

```text
DBT_BIGQUERY_PROJECT=YOUR_PROJECT_ID
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Do not commit service-account JSON keys to GitHub.

## Validate Connection

```bash
dbt debug
```

## Build Everything

```bash
dbt build
```

This runs models and tests in dependency order.

## Build Only Marts

```bash
dbt build --select path:models/marts
```

## Run One Model

```bash
dbt run --select mart_product_fit_performance
```

## Test One Model

```bash
dbt test --select mart_product_fit_performance
```

## Full Refresh

Use only when model logic changes require historical recomputation:

```bash
dbt run --full-refresh --select mart_product_fit_performance+
```

## Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

## Data Tests

The project validates:

- surrogate/natural keys are not null
- unique IDs remain unique
- only one current SCD2 row exists per customer/product
- purchase dimension keys resolve
- fit feedback stays within expected domains
- KPI rates remain between 0 and 1
