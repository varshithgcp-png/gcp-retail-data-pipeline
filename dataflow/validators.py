"""CSV parsing and row-level validation helpers."""

import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple


def parse_csv_line(line: str, columns: List[str]) -> Dict[str, str]:
    values = next(csv.reader([line]))
    if len(values) != len(columns):
        raise ValueError(
            f"Expected {len(columns)} columns but received {len(values)}"
        )
    return dict(zip(columns, values))


def _validate_date(value: str, field: str) -> None:
    if not value:
        return
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc


def _validate_timestamp(value: str, field: str) -> None:
    if not value:
        return
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601 timestamp") from exc


def validate_and_cast(
    row: Dict[str, str],
    config: Dict,
) -> Tuple[bool, Dict, str]:
    """Validate required values, types and configured domains."""

    for field in config["required"]:
        if row.get(field) is None or not str(row[field]).strip():
            return False, row, f"Missing required field: {field}"

    typed = dict(row)

    try:
        for field in config["integer_fields"]:
            if row.get(field):
                typed[field] = int(row[field])

        for field in config["numeric_fields"]:
            if row.get(field):
                Decimal(row[field])
                typed[field] = float(row[field])

        for field in config["date_fields"]:
            _validate_date(row.get(field, ""), field)

        for field in config["timestamp_fields"]:
            _validate_timestamp(row.get(field, ""), field)

        for field, allowed in config.get("allowed_values", {}).items():
            value = row.get(field, "")
            if value not in allowed:
                return False, row, f"Invalid {field}: {value}"

        if "quantity" in typed and typed["quantity"] <= 0:
            return False, row, "quantity must be greater than 0"

        for amount_field in ("amount", "return_amount", "unit_price"):
            if amount_field in typed and typed[amount_field] < 0:
                return False, row, f"{amount_field} cannot be negative"

        if "confidence_score" in typed:
            score = typed["confidence_score"]
            if score < 0 or score > 1:
                return False, row, "confidence_score must be between 0 and 1"

    except (ValueError, InvalidOperation) as exc:
        return False, row, str(exc)

    return True, typed, ""
