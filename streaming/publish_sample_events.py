"""Publish synthetic Fit Analytics recommendation events to Pub/Sub."""

import argparse
import json
import time
from pathlib import Path

from google.cloud import pubsub_v1


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
    args = parser.parse_args()

    topic_path = (
        f"projects/{args.project_id}/topics/{args.topic}"
    )

    publisher = pubsub_v1.PublisherClient()

    for line in Path(args.input_file).read_text().splitlines():
        if not line.strip():
            continue

        event = json.loads(line)

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
            f"message_id={future.result()}"
        )

        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
