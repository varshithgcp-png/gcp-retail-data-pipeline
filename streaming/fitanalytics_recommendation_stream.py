"""Fit Analytics real-time recommendation pipeline.

Pub/Sub -> Dataflow -> validation -> BigQuery
                         \-> BigQuery DLQ + Pub/Sub DLQ

The Pub/Sub message contract requires these attributes:
- event_id: stable logical event identifier used by Dataflow source deduplication
- event_ts: RFC3339 event time used for Dataflow watermarking/windowing
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp

try:
    from streaming.schemas import (
        RECOMMENDATION_EVENT_SCHEMA,
        STREAMING_DLQ_SCHEMA,
        WINDOW_METRIC_SCHEMA,
    )
    from streaming.validators import validate_recommendation_event
except ModuleNotFoundError:
    from schemas import (
        RECOMMENDATION_EVENT_SCHEMA,
        STREAMING_DLQ_SCHEMA,
        WINDOW_METRIC_SCHEMA,
    )
    from validators import validate_recommendation_event


def to_beam_timestamp(dt):
    if dt is None:
        return None
    if isinstance(dt, Timestamp):
        return dt
    return Timestamp(dt.timestamp())


class ParseValidateRecommendationFn(beam.DoFn):
    INVALID = "invalid"

    def __init__(self):
        self.valid_events = Metrics.counter(
            self.__class__,
            "valid_events",
        )
        self.invalid_events = Metrics.counter(
            self.__class__,
            "invalid_events",
        )

    def _dlq_row(self, message, raw_payload, error_message):
        now = datetime.now(timezone.utc)
        attributes = message.attributes or {}

        return {
            "event_id": attributes.get("event_id"),
            "raw_payload": raw_payload,
            "attributes_json": json.dumps(attributes, sort_keys=True),
            "error_message": error_message,
            "pubsub_message_id": message.message_id,
            "publish_time": to_beam_timestamp(message.publish_time),
            "ingestion_ts": to_beam_timestamp(now),
        }

    def process(self, message):
        raw_payload = ""

        try:
            raw_payload = message.data.decode("utf-8")
            event = json.loads(raw_payload)

            if not isinstance(event, dict):
                raise ValueError("Message payload must be a JSON object")

            is_valid, typed, error = validate_recommendation_event(event)
            if not is_valid:
                raise ValueError(error)

            attributes = message.attributes or {}
            attribute_event_id = attributes.get("event_id")
            attribute_event_ts = attributes.get("event_ts")

            if attribute_event_id != typed["event_id"]:
                raise ValueError(
                    "Pub/Sub event_id attribute must match payload event_id"
                )

            if attribute_event_ts != typed["event_ts"]:
                raise ValueError(
                    "Pub/Sub event_ts attribute must match payload event_ts"
                )

            now = datetime.now(timezone.utc)
            event_dt = typed.pop("_event_datetime")

            row = {
                "event_id": typed["event_id"],
                "recommendation_id": typed["recommendation_id"],
                "customer_id": typed["customer_id"],
                "product_id": typed["product_id"],
                "recommended_size": typed["recommended_size"],
                "confidence_score": typed["confidence_score"],
                "event_ts": to_beam_timestamp(event_dt),
                "pubsub_message_id": message.message_id,
                "publish_time": to_beam_timestamp(message.publish_time),
                "ingestion_ts": to_beam_timestamp(now),
                "event_lag_seconds": (
                    now - event_dt.astimezone(timezone.utc)
                ).total_seconds(),
                "source": "pubsub",
            }

            self.valid_events.inc()
            yield beam.window.TimestampedValue(
                row,
                event_dt.timestamp(),
            )

        except Exception as exc:
            self.invalid_events.inc()
            yield beam.pvalue.TaggedOutput(
                self.INVALID,
                self._dlq_row(message, raw_payload, str(exc)),
            )


class AddWindowMetadataFn(beam.DoFn):
    def process(self, row, window=beam.DoFn.WindowParam):
        output = dict(row)
        output["window_start"] = window.start
        output["window_end"] = window.end
        yield output


class BuildWindowMetricFn(beam.DoFn):
    def process(
        self,
        event_count,
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        yield {
            "window_start": window.start,
            "window_end": window.end,
            "event_count": event_count,
            "pane_index": pane_info.index,
            "pane_timing": str(pane_info.timing),
            "emitted_at": Timestamp(
                datetime.now(timezone.utc).timestamp()
            ),
        }


def dlq_to_pubsub_bytes(row):
    serializable = dict(row)

    for field in ("publish_time", "ingestion_ts"):
        value = serializable.get(field)
        if isinstance(value, Timestamp):
            serializable[field] = value.to_rfc3339()

    return json.dumps(
        serializable,
        sort_keys=True,
        default=str,
    ).encode("utf-8")


def storage_write(
    pcoll,
    table,
    schema,
    label,
):
    return pcoll | label >> beam.io.WriteToBigQuery(
        table,
        schema=schema,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
        triggering_frequency=5,
        with_auto_sharding=True,
        use_at_least_once=False,
    )


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--subscription",
        required=True,
        help=(
            "projects/PROJECT/subscriptions/SUBSCRIPTION for "
            "recommendation events"
        ),
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="PROJECT:DATASET.TABLE for validated recommendation events",
    )
    parser.add_argument(
        "--dlq_table",
        required=True,
        help="PROJECT:DATASET.TABLE for invalid events",
    )
    parser.add_argument(
        "--dlq_topic",
        required=True,
        help="projects/PROJECT/topics/TOPIC for invalid-event replay",
    )
    parser.add_argument(
        "--window_metrics_table",
        required=True,
        help="PROJECT:DATASET.TABLE for event-time window metrics",
    )
    parser.add_argument(
        "--window_seconds",
        type=int,
        default=300,
        help="Fixed event-time window size. Default: 300 seconds.",
    )
    parser.add_argument(
        "--allowed_lateness_seconds",
        type=int,
        default=600,
        help="Allowed lateness for window metrics. Default: 600 seconds.",
    )

    known_args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read Recommendation PubSub" >> ReadFromPubSub(
                subscription=known_args.subscription,
                with_attributes=True,
                id_label="event_id",
            )
        )

        parsed = (
            messages
            | "Parse Validate Recommendation" >> beam.ParDo(
                ParseValidateRecommendationFn()
            ).with_outputs(
                ParseValidateRecommendationFn.INVALID,
                main="valid",
            )
        )

        event_rows = (
            parsed.valid
            | "Recommendation Event Windows" >> beam.WindowInto(
                FixedWindows(known_args.window_seconds),
                allowed_lateness=known_args.allowed_lateness_seconds,
            )
            | "Add Recommendation Window Metadata" >> beam.ParDo(
                AddWindowMetadataFn()
            )
        )

        storage_write(
            event_rows,
            known_args.output_table,
            RECOMMENDATION_EVENT_SCHEMA,
            "Write Recommendation Events",
        )

        storage_write(
            parsed.invalid,
            known_args.dlq_table,
            STREAMING_DLQ_SCHEMA,
            "Write Streaming DLQ Table",
        )

        _ = (
            parsed.invalid
            | "Serialize DLQ PubSub" >> beam.Map(dlq_to_pubsub_bytes)
            | "Write DLQ PubSub" >> WriteToPubSub(
                known_args.dlq_topic
            )
        )

        windowed_for_metrics = (
            parsed.valid
            | "Window For Streaming Metrics" >> beam.WindowInto(
                FixedWindows(known_args.window_seconds),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(60),
                    late=trigger.AfterCount(1),
                ),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=known_args.allowed_lateness_seconds,
            )
        )

        window_metrics = (
            windowed_for_metrics
            | "Count Events Per Window" >> beam.combiners.Count.Globally()
                .without_defaults()
            | "Build Window Metrics" >> beam.ParDo(
                BuildWindowMetricFn()
            )
        )

        storage_write(
            window_metrics,
            known_args.window_metrics_table,
            WINDOW_METRIC_SCHEMA,
            "Write Window Metrics",
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
