# BigQuery Performance and Cost Optimization

## Partitioning

Use business/event dates because they match the most common analytical filters and support backfills cleanly.

Current examples:

- `fact_purchase` -> `purchase_date`
- `fact_return` -> `return_date`
- `fact_recommendation` -> `DATE(recommendation_ts)`
- streaming recommendations -> `DATE(event_ts)`
- reconciliation/audit -> audit date

Queries should filter directly on the partition column so BigQuery can prune partitions.

## Clustering

Use high-value filter/join columns after partitioning:

- `customer_id`
- `product_id`
- `brand`
- `category`
- `event_id`

Clustering is used where repeated analytical filters or joins benefit from colocating similar values.

## Query Practices

- avoid `SELECT *` in production reporting queries
- filter partitions early
- pre-aggregate before large joins where possible
- use surrogate keys for SCD2 fact-to-dimension joins
- inspect query execution details for shuffle, skew and large read stages
- use `INFORMATION_SCHEMA.JOBS*` to review bytes processed and slot usage
- materialize frequently reused transformations with dbt table/incremental models
- use incremental MERGE with a controlled lookback for late-arriving returns

## Cost Controls

- partition and cluster large fact tables
- use incremental dbt models instead of full rebuilds
- keep staging/history retention policies appropriate for the use case
- monitor bytes processed and slot consumption
- separate development and production datasets/projects where possible
- use dry runs for large ad-hoc queries before execution

## Example Partition-Pruned Query

```sql
SELECT
  product_id,
  COUNT(*) AS purchases,
  SUM(amount) AS revenue
FROM `PROJECT_ID.fitanalytics_curated.fact_purchase`
WHERE purchase_date BETWEEN DATE('2026-11-01') AND DATE('2026-11-14')
GROUP BY product_id;
```
