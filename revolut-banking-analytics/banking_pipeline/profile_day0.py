from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from banking_pipeline.parser import parse_interleaved_text

def profile(path="data/EvalDB_Day0_Interleaved.txt"):
    blocks = parse_interleaved_text(Path(path).read_text(encoding="utf-8"))
    idx = {e: {c: i for i, c in enumerate(b.header)} for e, b in blocks.items()}
    def v(e, row, c): return row[idx[e][c]]
    counts = {e: len(b.rows) for e, b in blocks.items()}
    customers = blocks["CUSTOMERS"].rows
    accounts = blocks["ACCOUNTS"].rows
    txns = blocks["TRANSACTIONS"].rows
    branches = {v("BRANCHES", r, "BranchID"): r for r in blocks["BRANCHES"].rows}
    acct_types = {v("ACCOUNTTYPES", r, "AccountTypeID"): r for r in blocks["ACCOUNTTYPES"].rows}
    acct = {v("ACCOUNTS", r, "AccountID"): r for r in accounts}
    txn_types = {v("TRANSACTIONTYPES", r, "TransactionTypeID"): r for r in blocks["TRANSACTIONTYPES"].rows}
    cust_counts = Counter(v("CUSTOMERS", r, "CustomerID") for r in customers)
    txn_counts = Counter(v("TRANSACTIONS", r, "TransactionID") for r in txns)
    result = {"entity_counts":counts,"total_rows":sum(counts.values()),"duplicate_customer_keys":sum(1 for n in cust_counts.values() if n > 1),"missing_critical_customers":sum(1 for r in customers if not v("CUSTOMERS", r, "Email").strip() or not v("CUSTOMERS", r, "Phone").strip()),"duplicate_transaction_keys":sum(1 for n in txn_counts.values() if n > 1)}
    currency_mismatch=invalid_date=high_value=0
    for r in txns:
        a=acct.get(v("TRANSACTIONS",r,"AccountID"))
        if a:
            if v("TRANSACTIONS",r,"Currency") != v("ACCOUNTS",a,"Currency"): currency_mismatch += 1
            td=datetime.fromisoformat(v("TRANSACTIONS",r,"TransactionTs")).date(); od=datetime.fromisoformat(v("ACCOUNTS",a,"OpenDate")).date(); close=v("ACCOUNTS",a,"CloseDate")
            if td < od or (close and td > datetime.fromisoformat(close).date()): invalid_date += 1
        if abs(Decimal(v("TRANSACTIONS",r,"Amount"))) > Decimal("100000"): high_value += 1
    result.update(currency_mismatch=currency_mismatch,invalid_transaction_dates=invalid_date,high_value_transactions=high_value)
    branch_bad=overdraft=0
    for r in accounts:
        b=branches.get(v("ACCOUNTS",r,"BranchID"))
        if b and not v("ACCOUNTS",r,"SortCode").startswith(v("BRANCHES",b,"SortCodePrefix")): branch_bad += 1
        at=acct_types.get(v("ACCOUNTS",r,"AccountTypeID"))
        if at and Decimal(v("ACCOUNTS",r,"Balance")) < -Decimal(v("ACCOUNTTYPES",at,"OverdraftLimit")): overdraft += 1
    result.update(branch_sort_code_mismatches=branch_bad,overdraft_breaches=overdraft)
    by_cash=defaultdict(list); all_by=defaultdict(list)
    for r in txns:
        aid=v("TRANSACTIONS",r,"AccountID"); ts=datetime.fromisoformat(v("TRANSACTIONS",r,"TransactionTs")); all_by[aid].append(ts)
        tt=txn_types.get(v("TRANSACTIONS",r,"TransactionTypeID")); amount=abs(Decimal(v("TRANSACTIONS",r,"Amount")))
        if tt and v("TRANSACTIONTYPES",tt,"IsCashProxy") == "TRUE" and v("TRANSACTIONS",r,"Channel") in ("BRANCH","ATM") and Decimal("9000") <= amount <= Decimal("10000"): by_cash[aid].append(ts)
    structuring=set()
    for aid,times in by_cash.items():
        for t in sorted(times):
            if sum(1 for x in times if 0 <= (t-x).total_seconds() <= 3600) >= 3: structuring.add(aid)
    dormant=set()
    for aid,times in all_by.items():
        a=acct.get(aid)
        if a and v("ACCOUNTS",a,"Status") == "DORMANT":
            for t in sorted(times):
                if sum(1 for x in times if 0 <= (t-x).total_seconds() <= 3600) >= 3: dormant.add(aid)
    result["structuring_candidate_accounts"] = sorted(structuring,key=int)
    result["dormant_reactivation_accounts"] = sorted(dormant,key=int)
    return result

if __name__ == "__main__":
    import json
    print(json.dumps(profile(),indent=2))
