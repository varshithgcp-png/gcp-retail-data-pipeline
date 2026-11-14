# Expected Synthetic Results

## Sample File Naming for Composer

The files under `data/` are repository-friendly sample names. When uploading to Composer's landing contract, use names matching the entity prefix:

| Repository sample | Example landing filename |
|---|---|
| `customers.csv` | `customer_20261101.csv` |
| `customer_updates.csv` | `customer_20261102.csv` |
| `products.csv` | `product_20261101.csv` |
| `purchases.csv` | `purchase_20261101.csv` |
| `returns.csv` | `return_20261105.csv` |
| `recommendations.csv` | `recommendation_20261103.csv` |


This document is the acceptance checklist for the sample data. It describes what should happen after the portfolio is deployed and executed.

## Customer SCD2

Run the customer samples as two different batches.

### Batch 1

Upload `data/customers.csv` as a file such as:

```text
landing/customer/business_date=2026-11-01/customer_20261101.csv
```

Expected current customer rows after Batch 1: **4**.

### Batch 2

Upload `data/customer_updates.csv` as:

```text
landing/customer/business_date=2026-11-02/customer_20261102.csv
```

Expected behaviour:

- C002 old row becomes `is_current = false`
- C002 new row has weight 60 and preferred fit REGULAR
- C005 is inserted as a new customer
- current customer rows = **5**
- total customer dimension rows = **6**
- C002 has **2 historical versions**

The two C002 versions are deliberately separated into different batches because SCD2 must observe the change across processing runs.

## Purchase Batch

Input rows: **6**

Expected transformation result:

- PU1004 appears twice -> one duplicate removed
- PU1005 references customer C999 -> transformation reject
- curated fact rows = **4**
- duplicate count = **1**
- transformation reject count = **1**

Expected reconciliation:

```text
stage_count     = 6
valid_count     = 4
rejected_count  = 1
duplicate_count = 1
target_count    = 4
status          = MATCHED
```

## Return Batch

Input rows: **4**

Expected transformation result:

- R2001 -> valid
- R2002 -> valid
- R2003 -> valid
- R2004 references PU9999 -> transformation reject
- curated return rows = **3**

Expected reconciliation:

```text
stage_count     = 4
valid_count     = 3
rejected_count  = 1
duplicate_count = 0
target_count    = 3
status          = MATCHED
```

## Batch Recommendation File

Input rows: **7**

Expected Dataflow result:

- REC3006 has confidence score 1.25 -> batch quarantine
- stage rows = **6**
- quarantine rows = **1**

Expected Dataproc result for the six staged rows:

- EVT-004 is duplicated -> one duplicate removed
- curated recommendation rows = **5**
- transformation duplicate count = **1**

## Fit KPI Sanity Check

Using valid purchases PU1001-PU1004 and valid returns R2001-R2003:

| Product | Purchases | Fit Returns | Expected Fit Return Rate | Recommendation Accepted |
|---|---:|---:|---:|---:|
| P501 | 1 | 0 | 0.00 | Yes |
| P502 | 1 | 1 | 1.00 | No |
| P503 | 1 | 1 | 1.00 | No |
| P504 | 1 | 0 | 0.00 | Yes |

For the batch recommendation sample:

- recommendations with a matched purchase = **4**
- correct-size matched purchases = **2**
- recommendation accuracy among matched purchases = **0.50**

REC3005 has no matching valid purchase and therefore contributes to recommendation/conversion volume but not the matched-purchase accuracy denominator.

## Streaming Sample

The JSONL sample includes:

- normal valid events
- duplicate EVT-S-004
- invalid confidence event EVT-S-005
- out-of-order older event EVT-S-006

Expected behaviour:

- EVT-S-005 -> streaming DLQ
- nearby duplicate EVT-S-004 -> source dedupe when Dataflow's ID-attribute dedupe applies
- persistent BigQuery consumer view keeps one row per event_id
- EVT-S-006 uses its validated event timestamp for windowing

Actual early/late pane timing depends on runtime watermark progression.
