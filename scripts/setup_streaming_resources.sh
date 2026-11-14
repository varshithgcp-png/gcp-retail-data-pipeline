#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${1:?Usage: $0 PROJECT_ID [REGION]}"
REGION="${2:-us-central1}"

EVENT_TOPIC="fitanalytics-recommendation-events"
EVENT_SUBSCRIPTION="fitanalytics-recommendation-sub"
DLQ_TOPIC="fitanalytics-recommendation-dlq"

gcloud config set project "${PROJECT_ID}"

gcloud pubsub topics describe "${EVENT_TOPIC}" >/dev/null 2>&1 ||   gcloud pubsub topics create "${EVENT_TOPIC}"

gcloud pubsub subscriptions describe "${EVENT_SUBSCRIPTION}" >/dev/null 2>&1 ||   gcloud pubsub subscriptions create "${EVENT_SUBSCRIPTION}"     --topic="${EVENT_TOPIC}"     --ack-deadline=60

gcloud pubsub topics describe "${DLQ_TOPIC}" >/dev/null 2>&1 ||   gcloud pubsub topics create "${DLQ_TOPIC}"

echo
echo "Streaming resources ready."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Input topic: ${EVENT_TOPIC}"
echo "Subscription: ${EVENT_SUBSCRIPTION}"
echo "Pipeline DLQ topic: ${DLQ_TOPIC}"
