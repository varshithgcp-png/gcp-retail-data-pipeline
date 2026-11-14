"""Validation helpers for real-time Fit Analytics recommendation events."""

from datetime import datetime
from typing import Any, Dict, Tuple

from streaming.schemas import REQUIRED_FIELDS


def parse_rfc3339(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_ts must be a non-empty RFC3339 timestamp")

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("event_ts must be a valid RFC3339 timestamp") from exc

    if parsed.tzinfo is None:
        raise ValueError("event_ts must include a timezone")

    return parsed


def validate_recommendation_event(
    event: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any], str]:
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            return False, event, f"Missing required field: {field}"

        if isinstance(event[field], str) and not event[field].strip():
            return False, event, f"Missing required field: {field}"

    typed = dict(event)

    try:
        typed["confidence_score"] = float(event["confidence_score"])
        if not 0 <= typed["confidence_score"] <= 1:
            return (
                False,
                event,
                "confidence_score must be between 0 and 1",
            )

        typed["_event_datetime"] = parse_rfc3339(event["event_ts"])

    except (TypeError, ValueError) as exc:
        return False, event, str(exc)

    return True, typed, ""
