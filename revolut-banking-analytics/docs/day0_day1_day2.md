# Day 0 -> Day 1 -> Day 2 Demonstration

## Day 0
Full baseline: all ten entities.

## Day 1
Only `CUSTOMERS` and `ACCOUNTS` blocks.
- New customers: 369, 370
- New accounts: 481, 482
- Existing customers/accounts are also changed to demonstrate SCD2.

## Day 2
Only `CUSTOMERS` and `ACCOUNTS` blocks.
- New customers: 371, 372
- New accounts: 483, 484
- Additional existing-record changes create another SCD2 version.

## Replay safety
The Python Bronze loader computes SHA-256 over the full source file. A successful file hash is skipped on replay. The batch ID is deterministic from business date + file hash, and partial attempts are cleaned before reload.

## Expected SCD2 evidence
After Day 0 -> Day 1 -> Day 2 snapshots:
- Customer 102 has multiple historical versions.
- Customer 369 is inserted on Day 1 and changes on Day 2.
- Account 12 has multiple historical versions.
- Account 481 is inserted on Day 1 and updated on Day 2.
- 370/371/372 and 482/483/484 demonstrate incremental inserts.
