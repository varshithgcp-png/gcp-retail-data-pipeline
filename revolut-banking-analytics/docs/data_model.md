# Gold Data Model

## Fact grains

### fact_transaction
**Grain:** one row per valid unique banking transaction.

Keys: customer, account, account type, branch, transaction type and date. Measures/attributes include amount, currency, channel and anomaly flags.

### fact_loan_payment
**Grain:** one row per unique loan repayment event.

Measures include payment amount, days late and repayment-risk flag.

## Dimensions

- `dim_customer` — SCD Type 2
- `dim_account` — SCD Type 2
- `dim_branch`
- `dim_account_type`
- `dim_transaction_type`
- `dim_date`
- `dim_card`
- `dim_loan`

## Business relationships

```text
Customers -> Accounts -> Transactions
Accounts -> Branches
Accounts -> AccountTypes
Transactions -> TransactionTypes
Customers -> Loans -> LoanPayments
Customers -> Accounts -> Cards
```
