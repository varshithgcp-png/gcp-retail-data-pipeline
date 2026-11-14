import csv
import json
from collections import Counter
from pathlib import Path


DATA = Path("data")


def read_csv(name):
    with (DATA / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_customer_scd2_samples_are_separate_batches():
    initial = read_csv("customers.csv")
    updates = read_csv("customer_updates.csv")

    assert len(initial) == 4
    assert {row["customer_id"] for row in initial} == {
        "C001", "C002", "C003", "C004"
    }
    assert {row["customer_id"] for row in updates} == {"C002", "C005"}

    c002_initial = next(row for row in initial if row["customer_id"] == "C002")
    c002_update = next(row for row in updates if row["customer_id"] == "C002")

    assert c002_initial["preferred_fit"] == "SLIM"
    assert c002_update["preferred_fit"] == "REGULAR"
    assert c002_initial["updated_at"] < c002_update["updated_at"]


def test_purchase_sample_contains_dedupe_and_ri_cases():
    purchases = read_csv("purchases.csv")
    ids = Counter(row["purchase_id"] for row in purchases)

    assert len(purchases) == 6
    assert ids["PU1004"] == 2
    assert any(row["customer_id"] == "C999" for row in purchases)


def test_return_sample_contains_unknown_purchase_case():
    returns = read_csv("returns.csv")
    assert any(row["purchase_id"] == "PU9999" for row in returns)


def test_batch_recommendations_contain_duplicate_and_quarantine_cases():
    rows = read_csv("recommendations.csv")
    event_ids = Counter(row["event_id"] for row in rows)

    assert event_ids["EVT-004"] == 2
    assert any(float(row["confidence_score"]) > 1 for row in rows)


def test_streaming_sample_contains_duplicate_invalid_and_out_of_order_events():
    events = [
        json.loads(line)
        for line in (DATA / "recommendation_events.jsonl").read_text().splitlines()
        if line.strip()
    ]

    event_ids = Counter(row["event_id"] for row in events)

    assert event_ids["EVT-S-004"] == 2
    assert any(float(row["confidence_score"]) > 1 for row in events)

    first_ts = events[0]["event_ts"]
    late_candidate_ts = next(
        row["event_ts"] for row in events if row["event_id"] == "EVT-S-006"
    )
    assert late_candidate_ts < first_ts
