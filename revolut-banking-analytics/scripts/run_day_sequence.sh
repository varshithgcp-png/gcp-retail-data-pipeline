#!/usr/bin/env bash
set -euo pipefail
: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
PROFILE_DIR="${DBT_PROFILES_DIR:-$HOME/.dbt}"

[[ -f data/EvalDB_Day0_Interleaved.txt ]] || python data/generate_day0.py

run_day () {
  local file="$1"
  local date="$2"
  local type="$3"
  python -m banking_pipeline.bronze_loader --project-id "$GCP_PROJECT_ID" --source "$file" --business-date "$date" --load-type "$type"
  (cd dbt && DBT_BIGQUERY_PROJECT="$GCP_PROJECT_ID" dbt build --select tag:silver --profiles-dir "$PROFILE_DIR")
  (cd dbt && DBT_BIGQUERY_PROJECT="$GCP_PROJECT_ID" dbt snapshot --profiles-dir "$PROFILE_DIR")
  (cd dbt && DBT_BIGQUERY_PROJECT="$GCP_PROJECT_ID" dbt build --select tag:gold --profiles-dir "$PROFILE_DIR")
  python -m banking_pipeline.mark_pipeline --project-id "$GCP_PROJECT_ID" --source "$file" --business-date "$date" --status SUCCESS
}

python -m banking_pipeline.seed_metadata --project-id "$GCP_PROJECT_ID"
run_day data/EvalDB_Day0_Interleaved.txt 2026-11-01 FULL
run_day data/EvalDB_Day1_Interleaved.txt 2026-11-02 INCREMENTAL
run_day data/EvalDB_Day2_Interleaved.txt 2026-11-03 INCREMENTAL
