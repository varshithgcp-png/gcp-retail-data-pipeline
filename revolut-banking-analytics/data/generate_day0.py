from pathlib import Path
from datetime import datetime, date, timedelta
import random

random.seed(42)
OUT = Path(__file__).with_name('EvalDB_Day0_Interleaved.txt')

branches=[('1','London Central','London','UK','10-20','London'),('2','Manchester City','Manchester','UK','20-30','North West'),('3','Birmingham Central','Birmingham','UK','30-40','West Midlands'),('4','Leeds Central','Leeds','UK','40-50','Yorkshire'),('5','Bristol Central','Bristol','UK','50-60','South West'),('6','Liverpool Central','Liverpool','UK','60-70','North West'),('7','Glasgow Central','Glasgow','UK','70-80','Scotland'),('8','Edinburgh Central','Edinburgh','UK','80-90','Scotland')]
first=['Oliver','Amelia','Noah','Isla','George','Ava','Leo','Mia','Harry','Sophia','Jack','Emily','Charlie','Grace','Oscar','Lily','Thomas','Ella','Henry','Freya']
last=['Smith','Jones','Taylor','Brown','Wilson','Davies','Evans','Thomas','Johnson','Roberts','Walker','Wright','Thompson','White','Hughes','Edwards','Green','Hall','Lewis','Harris']
cities=[b[2] for b in branches]
base=datetime(2026,11,1,8,0,0)
customers=[]
for cid in range(1,351):
    fn=random.choice(first); ln=random.choice(last)
    email=f'{fn.lower()}.{ln.lower()}{cid}@example.com'; phone=f'+4477{cid:08d}'[:13]
    dob=date(1965,1,1)+timedelta(days=random.randint(0,14000)); city=random.choice(cities)
    risk=random.choices(['LOW','MEDIUM','HIGH'],weights=[.65,.25,.10])[0]
    if cid%87==0: email=''
    customers.append((str(cid),fn,ln,email,phone,dob.isoformat(),city,'UK',risk,(base+timedelta(minutes=cid%120)).isoformat()))
customers.append(next(r for r in customers if r[0]=='120'))

employees=[]; eid=1
for b in branches:
    for _ in range(3):
        employees.append((str(eid),b[0],random.choice(first),random.choice(last),random.choice(['BranchManager','RelationshipManager','Teller','OperationsOfficer','RiskOfficer']),(date(2015,1,1)+timedelta(days=random.randint(0,3500))).isoformat(),'ACTIVE')); eid+=1

account_types=[('1','Current Account','CURRENT','5.00','0.10','500.00'),('2','Savings Account','SAVINGS','0.00','2.75','0.00'),('3','Premium Account','PREMIUM','15.00','1.25','2000.00'),('4','Student Account','STUDENT','0.00','0.25','1000.00'),('5','Business Account','BUSINESS','10.00','0.50','5000.00')]
accounts=[]; account_currency={}; account_open={}
for aid in range(1,481):
    cid=str(random.randint(1,350)); atid=str(random.randint(1,5)); branch=random.choice(branches)
    sort_code=branch[4]+f'-{random.randint(10,99)}'
    if aid%113==0: sort_code='99-99-'+f'{random.randint(10,99)}'
    currency=random.choices(['GBP','EUR','USD'],weights=[.90,.06,.04])[0]
    balance=round(random.uniform(-1500,50000),2)
    if aid%97==0: atid='1'; balance=-1250.00
    open_date=date(2016,1,1)+timedelta(days=random.randint(0,3200))
    status=random.choices(['ACTIVE','DORMANT','CLOSED'],weights=[.85,.10,.05])[0]
    if aid==17: status='DORMANT'
    close=''
    if status=='CLOSED': close=(open_date+timedelta(days=random.randint(200,1800))).isoformat()
    accounts.append((str(aid),cid,atid,branch[0],sort_code,f'{10000000+aid}',currency,f'{balance:.2f}',open_date.isoformat(),close,status,(base+timedelta(minutes=aid%180)).isoformat()))
    account_currency[str(aid)]=currency; account_open[str(aid)]=open_date

cards=[]; card_id=1
for aid in range(1,481):
    if random.random()<.65:
        issue=date(2022,1,1)+timedelta(days=random.randint(0,1200)); expiry=issue.replace(year=issue.year+4)
        cards.append((str(card_id),str(aid),random.choice(['DEBIT','CREDIT']),random.choice(['VISA','MASTERCARD']),issue.isoformat(),expiry.isoformat(),random.choices(['ACTIVE','BLOCKED','EXPIRED'],weights=[.9,.05,.05])[0],random.choice(['TRUE','FALSE']))); card_id+=1

types=[('1','Salary Credit','INCOME','CREDIT','FALSE'),('2','Card Purchase','CARD_PAYMENT','DEBIT','FALSE'),('3','ATM Withdrawal','ATM','DEBIT','TRUE'),('4','Bank Transfer Out','TRANSFER','DEBIT','FALSE'),('5','Bank Transfer In','TRANSFER','CREDIT','FALSE'),('6','Cash Deposit','DEPOSIT','CREDIT','TRUE'),('7','Loan Repayment','LOAN_PAYMENT','DEBIT','FALSE'),('8','Fee Charge','FEE','DEBIT','FALSE')]
transactions=[]; start=datetime(2026,7,1,8)
for tid in range(1,1201):
    aid=str(random.randint(1,480)); ttype=str(random.randint(1,8)); ts=start+timedelta(minutes=random.randint(0,60*24*120)); amount=round(random.uniform(5,15000),2); currency=account_currency[aid]; channel=random.choice(['MOBILE','WEB','CARD','ATM','BRANCH','BACS','SYSTEM']); merchant=random.choice(['GROCERIES','TRAVEL','RETAIL','RESTAURANT','UTILITIES','TRANSFER','CASH','FEE','SALARY','ELECTRONICS'])
    if tid%211==0: amount=125000.00
    if tid%173==0: currency='EUR' if account_currency[aid] != 'EUR' else 'USD'
    if tid%257==0: ts=datetime.combine(account_open[aid]-timedelta(days=2),datetime.min.time())+timedelta(hours=10)
    if tid in (1001,1002,1003):
        aid='8'; ttype='6'; amount={1001:9800,1002:9700,1003:9600}[tid]; ts=datetime(2026,10,31,10)+timedelta(minutes=(tid-1001)*10); currency=account_currency[aid]; channel='ATM' if tid==1002 else 'BRANCH'; merchant='CASH'
    if tid in (1101,1102,1103):
        aid='17'; ttype='6'; amount={1101:9200,1102:9300,1103:9400}[tid]; ts=datetime(2026,10,30,9)+timedelta(minutes=(tid-1101)*12); currency=account_currency[aid]; channel='ATM' if tid==1103 else 'BRANCH'; merchant='CASH'
    transactions.append((str(tid),aid,ttype,ts.isoformat(timespec='seconds'),f'{amount:.2f}',currency,channel,merchant,f'REF-{tid:06d}'))
transactions.append(transactions[17])

loans=[]
for lid in range(1,81):
    cid=str(random.randint(1,350)); branch=random.choice(branches); principal=round(random.uniform(5000,250000),2); sd=date(2022,1,1)+timedelta(days=random.randint(0,1200)); ed=sd.replace(year=sd.year+random.choice([3,5,10,20])); status=random.choices(['ACTIVE','CLOSED','OVERDUE'],weights=[.75,.15,.10])[0]
    loans.append((str(lid),cid,branch[0],random.choice(['PERSONAL','AUTO','MORTGAGE']),f'{principal:.2f}',f'{random.uniform(3.5,10):.2f}',sd.isoformat(),ed.isoformat(),status))

payments=[]; pid=1
for lid in range(1,81):
    for _ in range(random.randint(2,5)):
        due=date(2026,7,1)+timedelta(days=random.randint(0,120)); status=random.choices(['ON_TIME','LATE','OVERDUE'],weights=[.7,.2,.1])[0]; delay=0 if status=='ON_TIME' else random.randint(2,25); pd=due+timedelta(days=delay)
        payments.append((str(pid),str(lid),pd.isoformat(),due.isoformat(),f'{random.uniform(150,1800):.2f}',status)); pid+=1

sections=[]
def add(name,header,rows):
    sections.append(f'HDR|{name}'); sections.append('|'.join(header)); sections.extend('|'.join(map(str,r)) for r in rows); sections.append(f'FTR|{name}'); sections.append('')
add('BRANCHES',['BranchID','BranchName','City','Country','SortCodePrefix','Region'],branches)
add('CUSTOMERS',['CustomerID','FirstName','LastName','Email','Phone','DateOfBirth','City','Country','RiskRating','UpdatedAt'],customers)
add('EMPLOYEES',['EmployeeID','BranchID','FirstName','LastName','Role','HireDate','Status'],employees)
add('ACCOUNTTYPES',['AccountTypeID','AccountType','ProductCategory','MonthlyFee','InterestRate','OverdraftLimit'],account_types)
add('ACCOUNTS',['AccountID','CustomerID','AccountTypeID','BranchID','SortCode','AccountNumber','Currency','Balance','OpenDate','CloseDate','Status','UpdatedAt'],accounts)
add('CARDS',['CardID','AccountID','CardType','Network','IssueDate','ExpiryDate','Status','ContactlessEnabled'],cards)
add('TRANSACTIONTYPES',['TransactionTypeID','TransactionType','Category','Direction','IsCashProxy'],types)
add('TRANSACTIONS',['TransactionID','AccountID','TransactionTypeID','TransactionTs','Amount','Currency','Channel','MerchantCategory','Reference'],transactions)
add('LOANS',['LoanID','CustomerID','BranchID','LoanType','PrincipalAmount','InterestRate','StartDate','EndDate','Status'],loans)
add('LOANPAYMENTS',['PaymentID','LoanID','PaymentDate','DueDate','Amount','PaymentStatus'],payments)
OUT.write_text('\n'.join(sections),encoding='utf-8')
print(f'Wrote {OUT}')
