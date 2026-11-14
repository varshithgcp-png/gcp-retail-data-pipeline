# Phase 4 - dbt on BigQuery

## Purpose

Phase 4 converts curated batch data plus deduplicated streaming recommendations into tested reporting marts and business-facing views.

```text
fitanalytics_curated --------+
                             |
fitanalytics_streaming ------+--> dbt staging/unified views
                                      |
                                      v
                                  KPI marts
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
- `stg_recommendation_unified` - batch + streaming recommendations, deduplicated by `event_id`

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
- Recommendation Conversion Rate
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

The normal purchase lookback is configurable through:

```yaml
vars:
  return_lookback_days: 30
```

The model also detects newly ingested return rows by `created_at` and explicitly reprocesses their purchase IDs. This means a genuinely late return can refresh an older purchase even when that purchase is outside the normal date lookback.

## Authentication

The example profile uses:

```yaml
method: oauth
```

For local development, configure Application Default Credentials with `gcloud auth application-default login`.

Inside Google Cloud, dbt can use the workload's configured identity/ADC. Do not commit service-account JSON keys.

Set:

```text
DBT_BIGQUERY_PROJECT=YOUR_PROJECT_ID
```

## Setup

From the repository root:

```bash
python -m venv .venv
pip install -r requirements.txt
cd dbt
dbt deps
```

Copy `profiles.yml.example` to your dbt profiles directory as `profiles.yml`.

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
- unified recommendation events resolve to customer/product dimensions
- fit feedback stays within expected domains
- KPI rates remain between 0 and 1
