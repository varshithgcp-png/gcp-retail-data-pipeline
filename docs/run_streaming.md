# Phase 6 - Pub/Sub + Dataflow Streaming

## Goal

Build the real-time Fit Analytics recommendation pipeline:

```text
Recommendation Events
        |
        v
      Pub/Sub
        |
        v
Dataflow / Apache Beam
        |
   +----+-------------------+
   |                        |
 valid                    invalid
   |                        |
   v                        v
BigQuery               BigQuery DLQ
Storage Write API           +
   |                    Pub/Sub DLQ
   v
5-minute event-time windows
   |
   v
Streaming Window Metrics
```

This streaming path runs independently from the twice-daily batch Composer DAG.

## Event Contract

Each Pub/Sub message contains a JSON payload:

```json
{
  "event_id": "EVT-S-001",
  "recommendation_id": "REC-S-001",
  "customer_id": "C001",
  "product_id": "P501",
  "recommended_size": "M",
  "confidence_score": 0.94,
  "event_ts": "2026-11-14T08:00:10Z"
}
```

The publisher must also set two Pub/Sub message attributes:

```text
event_id = EVT-S-001
event_ts = 2026-11-14T08:00:10Z
```

The payload and attributes must match.

## Why Attributes Are Used

The Dataflow Pub/Sub source is configured with:

```python
id_label="event_id"
timestamp_attribute="event_ts"
```

The stable `event_id` lets the source deduplicate logical retries even when Pub/Sub assigns a different message ID.

The `event_ts` attribute tells Dataflow to use source event time for watermarking instead of only Pub/Sub publish time.

## Dedupe Strategy

There are two levels:

1. **Pub/Sub/Dataflow source dedupe**
   - logical key: `event_id`
   - protects against immediate publisher retries

2. **Warehouse business-key controls**
   - `recommendation_id` / `event_id`
   - protects downstream processing and reporting

The sample JSONL file contains `EVT-S-004` twice to demonstrate source-level deduplication.

## Windowing and Late Data

The pipeline uses:

```text
Fixed window      = 5 minutes
Allowed lateness  = 10 minutes
Early metric pane = after 60 seconds processing time
Late metric pane  = after each late event
Accumulation      = accumulating
```

The raw validated event is written once to the streaming event table. Window triggers are used for the separate monitoring-metrics branch.

## Explicit DLQ Pattern

Invalid business payloads are routed to:

```text
fitanalytics_audit.streaming_dlq
projects/.../topics/fitanalytics-recommendation-dlq
```

The project intentionally does **not** configure a Pub/Sub subscription dead-letter policy for the Dataflow input subscription. The application pipeline owns the dead-letter route.

## BigQuery Write Method

The validated stream and streaming audit tables use:

```text
BigQuery Storage Write API
Exactly-once write mode
Auto sharding
5-second commit frequency
```

The tables are created by:

```text
bigquery/create_streaming_tables.sql
```

## 1. Create Pub/Sub Resources

From the repository root:

```bash
bash scripts/setup_streaming_resources.sh YOUR_PROJECT_ID
```

This creates:

- `fitanalytics-recommendation-events`
- `fitanalytics-recommendation-sub`
- `fitanalytics-recommendation-dlq`

## 2. Create BigQuery Tables

Run:

```text
bigquery/create_streaming_tables.sql
```

Replace `PROJECT_ID` first.

## 3. Run the Streaming Pipeline

```bash
python -m streaming.fitanalytics_recommendation_stream \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --region us-central1 \
  --streaming \
  --job_name fitanalytics-recommendation-stream \
  --subscription projects/YOUR_PROJECT_ID/subscriptions/fitanalytics-recommendation-sub \
  --output_table YOUR_PROJECT_ID:fitanalytics_streaming.recommendation_events \
  --dlq_table YOUR_PROJECT_ID:fitanalytics_audit.streaming_dlq \
  --dlq_topic projects/YOUR_PROJECT_ID/topics/fitanalytics-recommendation-dlq \
  --window_metrics_table YOUR_PROJECT_ID:fitanalytics_streaming.recommendation_window_metrics \
  --window_seconds 300 \
  --allowed_lateness_seconds 600 \
  --temp_location gs://YOUR_BUCKET/dataflow/streaming/temp \
  --staging_location gs://YOUR_BUCKET/dataflow/streaming/staging \
  --setup_file ./setup.py
```

## 4. Publish Sample Events

Install dependencies:

```bash
pip install -r requirements.txt
```

Publish the synthetic events. By default the publisher shifts the sample timestamps near current UTC while preserving relative timing, including the intentionally late event:

```bash
python -m streaming.publish_sample_events \
  --project_id YOUR_PROJECT_ID \
  --topic fitanalytics-recommendation-events
```

## 5. Expected Behaviour

The sample file contains:

- normal valid recommendation events
- duplicate `EVT-S-004`
- `EVT-S-005` with confidence score `1.25`
- `EVT-S-006` published last but carrying an older event timestamp to demonstrate late data

Expected outcome:

- valid events -> `fitanalytics_streaming.recommendation_events`
- duplicate logical event -> source dedupe
- invalid confidence event -> `fitanalytics_audit.streaming_dlq` + Pub/Sub DLQ topic
- older `EVT-S-006` -> processed using event time and the configured allowed-lateness policy
- window counts -> `fitanalytics_streaming.recommendation_window_metrics`

## 6. Query Recent Events

```sql
SELECT
  event_id,
  customer_id,
  product_id,
  recommended_size,
  confidence_score,
  event_ts,
  event_lag_seconds
FROM `YOUR_PROJECT_ID.fitanalytics_streaming.recommendation_events`
WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY event_ts DESC;
```

## 7. Query Streaming DLQ

```sql
SELECT
  event_id,
  error_message,
  raw_payload,
  ingestion_ts
FROM `YOUR_PROJECT_ID.fitanalytics_audit.streaming_dlq`
ORDER BY ingestion_ts DESC;
```

## 8. Query Window Metrics

```sql
SELECT
  window_start,
  window_end,
  event_count,
  pane_index,
  pane_timing,
  emitted_at
FROM `YOUR_PROJECT_ID.fitanalytics_streaming.recommendation_window_metrics`
ORDER BY window_start DESC, pane_index DESC;
```

## Production Notes

- Use one dedicated Pub/Sub subscription per Dataflow pipeline.
- Keep publisher-generated `event_id` stable across retries.
- Keep `event_ts` in RFC3339 UTC format.
- Monitor Pub/Sub backlog and Dataflow watermark lag.
- Use IAM/service accounts rather than committing credentials.
- Do not use Pub/Sub Seek backward while the streaming pipeline is actively consuming; stop/restart the pipeline for controlled replay.
