# Assumptions and Decisions

- This is a synthetic Revolut-style banking portfolio project, not Revolut internal architecture or data.
- No Dataflow or Dataproc is used by request.
- Airflow orchestrates only; the Python ingestion program itself reads GCS and writes BigQuery Bronze.
- Bronze keeps source business fields as STRING so type enforcement is owned by dbt Silver.
- Customer and Account use SCD Type 2 through dbt snapshots.
- Only a business key whose **first observed version came from the Day 0 FULL load** is backdated to 1900-01-01. New Day 1/Day 2 customers and accounts begin at their source `UpdatedAt` timestamp.
- Account balance is present in the Account SCD2 source. In a large production bank, balance would usually be modelled separately as a periodic snapshot fact.
- Structuring is a review flag, not a determination of wrongdoing. The portfolio uses configurable £9,000-£10,000 **deposit** activity, a cash-proxy transaction type, Branch/ATM channels, 3+ events and a one-hour window.
- Dormant reactivation requires a dormant account, a 3+ transaction one-hour burst, and at least 90 days (or no prior activity in the observed history) before that burst.
- Product-wise "revenue contribution" is implemented as transaction-value contribution because the supplied business question asks which account types contribute most transaction value. It is not accounting revenue.
