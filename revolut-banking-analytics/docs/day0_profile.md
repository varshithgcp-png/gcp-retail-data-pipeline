# Day 0 Profiling Results

The synthetic Day 0 baseline contains **2,748 data rows** across all ten entity blocks. The profiling script is `banking_pipeline/profile_day0.py`.

| Entity | Rows |
|---|---:|
| BRANCHES | 8 |
| CUSTOMERS | 351 |
| EMPLOYEES | 24 |
| ACCOUNTTYPES | 5 |
| ACCOUNTS | 480 |
| CARDS | 308 |
| TRANSACTIONTYPES | 8 |
| TRANSACTIONS | 1,201 |
| LOANS | 80 |
| LOANPAYMENTS | 283 |

## Deliberate DQ / anomaly evidence

| Check | Day 0 evidence |
|---|---:|
| Duplicate customer identity keys | 1 |
| Missing critical customer rows | 4 |
| Exact duplicate transaction keys | 1 |
| Currency mismatch rows | 6 |
| Invalid transaction date rows | 55 |
| Branch/sort-code mismatches | 4 |
| Negative balance beyond overdraft | 11 |
| High-value transactions > £100,000 | 5 |
| Structuring candidate accounts | 2 (`8`, `17`) |
| Dormant reactivation accounts | 1 (`17`) |

These are intentional synthetic examples so dbt Silver produces traceable DQ/anomaly evidence rather than an unrealistically clean dataset. Account `17` is generated with no ordinary transactions before its deliberate October burst, so the dormant-reactivation case represents a genuine long-inactivity scenario.
