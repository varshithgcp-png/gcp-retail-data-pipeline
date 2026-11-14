# Full Fit Analytics Use-Case Review

## Review Status

The repository has been rechecked end-to-end across:

- synthetic source data
- batch file validation
- Dataflow batch ingestion
- BigQuery staging
- Dataproc/PySpark transformations
- SCD Type 2
- fact referential integrity
- reconciliation
- dbt KPI modelling
- Pub/Sub/Dataflow streaming
- Cloud Composer orchestration
- BigQuery optimization/security/monitoring
- GitHub Actions CI/CD

## Corrections Made During Review

### 1. CI package import failure

The first CI version ran tests without installing the repository package.

**Fix:** CI now installs the repo with `pip install -e .` before tests.

### 2. File-level validation was documented but incomplete

The DAG originally waited for a GCS prefix but did not verify file naming, extension, size or header.

**Fix:** Composer now validates:

- `entity_*.csv` naming
- CSV extension
- non-zero size
- exact expected header/order

before Dataflow starts.

### 3. Return referential integrity

Returns originally checked only customer and product IDs.

**Fix:** return transformations also require the referenced `purchase_id` to exist in curated purchases.

### 4. SCD2 sample history

Two C002 customer versions were originally placed in one source file. The batch dedupe step would keep only the latest version before SCD2.

**Fix:** initial customer data and customer updates are now separate synthetic batches.

### 5. Historical dimension backfills

Simple expire-and-insert SCD2 logic can mishandle an older dimension batch arriving after a newer version.

**Fix:** affected customer/product histories are rebuilt from retained work history ordered by source `updated_at`. Surrogate keys remain deterministic from business key + version start timestamp.

**Operational note:** if a dimension backfill changes historical version boundaries, rerun impacted fact business dates so their dimension surrogate keys are refreshed.

### 6. KPI double counting

Joining purchases directly to multiple return rows could inflate revenue and recommendation-acceptance calculations.

**Fix:** return rows are aggregated to purchase grain before product and regional KPI aggregation.

### 7. Late returns

A fixed small date lookback alone could miss a newly ingested return for an older purchase.

**Fix:** the product KPI mart uses a configurable 30-day normal lookback and explicitly reprocesses purchase IDs referenced by newly ingested return rows.

### 8. Streaming recommendations were isolated

The real-time Dataflow table existed, but dbt recommendation KPIs only consumed the batch curated fact.

**Fix:** `stg_recommendation_unified` combines batch recommendations with the deduplicated streaming recommendation view, deduplicates by `event_id`, and enriches streaming records with as-of SCD2 customer/product surrogate keys.

### 9. Streaming timestamp validation

Reading a custom Pub/Sub timestamp attribute directly at the source can make malformed timestamps difficult to route through application-level DLQ logic.

**Fix:** the pipeline validates `event_ts` itself and emits valid rows as Beam `TimestampedValue` elements before event-time windows.

### 10. dbt authentication

The earlier example depended on a service-account JSON key.

**Fix:** the example profile now uses OAuth/Application Default Credentials, avoiding committed key files.

## Synthetic Acceptance Scenarios

See `docs/expected_results.md`.

Key scenarios include:

- customer C002 SCD2 change across two batches
- duplicate purchase PU1004
- invalid purchase customer C999
- invalid return purchase PU9999
- duplicate batch recommendation EVT-004
- batch confidence score > 1 routed to quarantine
- duplicate streaming event EVT-S-004
- invalid streaming confidence score routed to DLQ
- out-of-order streaming event candidate EVT-S-006

## Remaining Intentional Assumptions

These are design assumptions, not hidden failures.

### Recommendation-to-purchase attribution

The synthetic purchase source has day-grain `purchase_date`, not a recommendation/session ID. Recommendation conversion is therefore attributed by:

- same customer
- same product
- purchase date on/after recommendation date
- within 30 days
- earliest matching purchase

A production retailer feed with a recommendation/session ID or precise purchase timestamp should use that stronger key instead.

### Batch file contract

The Composer DAG expects a file for each of the five entities at each scheduled arrival window. If an entity has no data for that window, the upstream contract can provide a valid header-only CSV; a zero-row delta is allowed.

### Dimension extract grain

Within one dimension batch, the transformation keeps the latest row per business key. True historical changes for the same key should arrive as separate source update timestamps/batches if every intermediate SCD2 version must be preserved.

### Recommendation attribution

The portfolio attribution is intentionally heuristic because the purchase sample does not contain a recommendation/session ID. More than one recommendation can theoretically map to the same later purchase. A production feed with a recommendation ID or precise session/purchase timestamp should use that direct relationship.

### Streaming lateness demo

The sample includes an out-of-order older event. Whether it appears in an actual **late pane** depends on Dataflow watermark progression at runtime.

### Live-cloud execution

The codebase and CI can be validated in GitHub, but this repository has not yet been executed end-to-end in the owner's live GCP environment. Dataflow, Dataproc, Composer, BigQuery and Pub/Sub screenshots should only be added after real deployment.

## Deployment Acceptance

Before calling the project deployed, complete:

1. BigQuery DDLs.
2. Pub/Sub resources.
3. GCS landing/archive/temp layout.
4. Batch 1 customer load.
5. Batch 2 customer update load.
6. Purchase/return/recommendation batch.
7. Dataproc transformations and reconciliation.
8. dbt build.
9. Composer manual run/backfill.
10. Streaming Dataflow run and sample publisher.
11. Compare outputs with `docs/expected_results.md`.
