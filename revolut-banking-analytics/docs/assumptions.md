# Assumptions and Decisions

- This is a synthetic Revolut-style banking portfolio project, not Revolut internal architecture or data.
- No Dataflow or Dataproc is used by request.
- Airflow orchestrates only; the Python ingestion program itself reads GCS and writes BigQuery Bronze.
- Bronze keeps source business fields as STRING so type enforcement is owned by dbt Silver.
- Customer and Account use SCD Type 2 through dbt snapshots.
- The initial Day 0 version is treated as historically valid from 1900-01-01 so baseline transactions before the extract date can resolve to a dimension version.
- Account balance is present in the Account SCD2 source. In a large production bank, balance would usually be modelled separately as a periodic snapshot fact.
- Structuring is a review flag, not a determination of wrongdoing. The portfolio uses a configurable £9,000-£10,000 cash-proxy range, 3+ events, one-hour window.
- Product-wise "revenue contribution" is implemented as transaction-value contribution because the supplied business question asks which account types contribute most transaction value. It is not accounting revenue.
