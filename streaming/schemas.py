"""Schemas and event contract for Fit Analytics recommendation streaming."""

REQUIRED_FIELDS = [
    "event_id",
    "recommendation_id",
    "customer_id",
    "product_id",
    "recommended_size",
    "confidence_score",
    "event_ts",
]

RECOMMENDATION_EVENT_SCHEMA = (
    "event_id:STRING,"
    "recommendation_id:STRING,"
    "customer_id:STRING,"
    "product_id:STRING,"
    "recommended_size:STRING,"
    "confidence_score:FLOAT,"
    "event_ts:TIMESTAMP,"
    "pubsub_message_id:STRING,"
    "publish_time:TIMESTAMP,"
    "ingestion_ts:TIMESTAMP,"
    "event_lag_seconds:FLOAT,"
    "window_start:TIMESTAMP,"
    "window_end:TIMESTAMP,"
    "source:STRING"
)

STREAMING_DLQ_SCHEMA = (
    "event_id:STRING,"
    "raw_payload:STRING,"
    "attributes_json:STRING,"
    "error_message:STRING,"
    "pubsub_message_id:STRING,"
    "publish_time:TIMESTAMP,"
    "ingestion_ts:TIMESTAMP"
)

WINDOW_METRIC_SCHEMA = (
    "window_start:TIMESTAMP,"
    "window_end:TIMESTAMP,"
    "event_count:INTEGER,"
    "pane_index:INTEGER,"
    "pane_timing:STRING,"
    "emitted_at:TIMESTAMP"
)
