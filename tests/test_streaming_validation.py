from streaming.validators import validate_recommendation_event


def base_event():
    return {
        "event_id": "EVT-001",
        "recommendation_id": "REC-001",
        "customer_id": "C001",
        "product_id": "P501",
        "recommended_size": "M",
        "confidence_score": 0.94,
        "event_ts": "2026-11-14T08:00:10Z",
    }


def test_valid_recommendation_event():
    valid, typed, error = validate_recommendation_event(base_event())

    assert valid is True
    assert typed["confidence_score"] == 0.94
    assert typed["_event_datetime"].tzinfo is not None
    assert error == ""


def test_invalid_confidence_score():
    event = base_event()
    event["confidence_score"] = 1.25

    valid, _, error = validate_recommendation_event(event)

    assert valid is False
    assert error == "confidence_score must be between 0 and 1"


def test_missing_event_id():
    event = base_event()
    del event["event_id"]

    valid, _, error = validate_recommendation_event(event)

    assert valid is False
    assert error == "Missing required field: event_id"


def test_timestamp_requires_timezone():
    event = base_event()
    event["event_ts"] = "2026-11-14T08:00:10"

    valid, _, error = validate_recommendation_event(event)

    assert valid is False
    assert error == "event_ts must include a timezone"
