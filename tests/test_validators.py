from dataflow.schemas import ENTITY_CONFIG
from dataflow.validators import parse_csv_line, validate_and_cast


def test_valid_customer():
    config = ENTITY_CONFIG["customer"]
    line = "C001,UK,178,76,REGULAR,2026-11-01T08:10:00"

    row = parse_csv_line(line, config["columns"])
    valid, typed, error = validate_and_cast(row, config)

    assert valid is True
    assert typed["height_cm"] == 178
    assert error == ""


def test_invalid_customer_fit_domain():
    config = ENTITY_CONFIG["customer"]
    line = "C001,UK,178,76,UNKNOWN,2026-11-01T08:10:00"

    row = parse_csv_line(line, config["columns"])
    valid, _, error = validate_and_cast(row, config)

    assert valid is False
    assert error == "Invalid preferred_fit: UNKNOWN"


def test_invalid_recommendation_score():
    config = ENTITY_CONFIG["recommendation"]
    line = "REC1,C001,P501,M,1.5,2026-11-01T09:55:00,EVT-1"

    row = parse_csv_line(line, config["columns"])
    valid, _, error = validate_and_cast(row, config)

    assert valid is False
    assert error == "confidence_score must be between 0 and 1"


def test_wrong_column_count():
    config = ENTITY_CONFIG["product"]

    try:
        parse_csv_line("P501,TSHIRT-001,Aurora", config["columns"])
        assert False, "Expected parsing to fail"
    except ValueError as exc:
        assert "Expected 9 columns" in str(exc)
