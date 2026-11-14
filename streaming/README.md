# Fit Analytics Real-Time Recommendation Pipeline

## Architecture

```text
Publisher
   |
   v
Pub/Sub Topic
   |
   v
Dedicated Subscription
   |
   v
Dataflow Streaming
   |
   +--> Valid -> BigQuery Storage Write API
   |
   +--> Invalid -> BigQuery DLQ + Pub/Sub DLQ
   |
   +--> 5-minute Windows -> Streaming Metrics
```

## Main File

`fitanalytics_recommendation_stream.py`

## Key Concepts Demonstrated

- Pub/Sub subscription ingestion
- stable `event_id` deduplication
- validated event-time timestamps assigned with TimestampedValue
- watermarks
- fixed windows
- early/late triggers for monitoring metrics
- allowed lateness
- validation and explicit dead-letter routing
- BigQuery Storage Write API
- exactly-once BigQuery sink configuration
- auto sharding
- Beam metrics counters

See `docs/run_streaming.md` for setup and execution.
