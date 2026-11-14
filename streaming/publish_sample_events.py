"""Publish synthetic Fit Analytics recommendation events to Pub/Sub."""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import pubsub_v1


def parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_ts(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--input_file",
        default="data/recommendation_events.jsonl",
    )
    parser.add_argument(
        "--sleep_seconds",
        type=float,
        default=0.25,
    )
    parser.add_argument(
        "--preserve_event_times",
        action="store_true",
        help=(
            "Keep timestamps from the JSONL file. By default, timestamps "
            "are shifted so the first sample event occurs near current UTC."
        ),
    )
    args = parser.parse_args()

    topic_path = (
        f"projects/{args.project_id}/topics/{args.topic}"
    )

    events = [
        json.loads(line)
        for line in Path(args.input_file).read_text().splitlines()
        if line.strip()
    ]

    if not events:
        raise ValueError("No sample events found")

    if not args.preserve_event_times:
        first_event_ts = parse_ts(events[0]["event_ts"])
        shift = datetime.now(timezone.utc) - first_event_ts

        for event in events:
            event["event_ts"] = format_ts(
                parse_ts(event["event_ts"]) + shift
            )

    publisher = pubsub_v1.PublisherClient()

    for event in events:
        # Dataflow uses these attributes for logical-event dedupe
        # and event-time watermarking.
        attributes = {
            "event_id": str(event["event_id"]),
            "event_ts": str(event["event_ts"]),
        }

        future = publisher.publish(
            topic_path,
            json.dumps(event).encode("utf-8"),
            **attributes,
        )

        print(
            f"Published event_id={event['event_id']} "
            f"event_ts={event['event_ts']} "
            f"message_id={future.result()}"
        )

        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
