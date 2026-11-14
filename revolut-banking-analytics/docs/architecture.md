# Architecture

```text
Core Banking Extract
      |
      v
One interleaved HDR/FTR file in GCS
      |
      v
Airflow / Composer (orchestration only)
      |
      v
Python parser + BigQuery client
      |
      v
revolut_bronze raw tables + CONTROL_TABLE_bronze
      |
      v
dbt Silver: typing, cleansing, dedupe, RI, DQ, anomalies, quarantine evidence
      |
      v
dbt snapshots: Customer + Account SCD Type 2
      |
      v
dbt Gold: dimensions + facts + seven required KPI outputs
```

No Dataflow or Dataproc is used in this project.
