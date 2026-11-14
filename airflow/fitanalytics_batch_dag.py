"""Cloud Composer DAG for the Fit Analytics batch pipeline.

Scheduled for 08:00 and 13:00 Asia/Kolkata.
Manual reruns/backfills can override business_date and batch_id in dag_run.conf.
"""

import csv
import logging
import os
from datetime import timedelta

from google.cloud import storage

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowConfiguration,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator,
)
from airflow.utils.task_group import TaskGroup


PROJECT_ID = os.environ.get("FITANALYTICS_PROJECT_ID", "YOUR_PROJECT_ID")
REGION = os.environ.get("FITANALYTICS_REGION", "us-central1")
BQ_LOCATION = os.environ.get("FITANALYTICS_BQ_LOCATION", "US")
GCS_BUCKET = os.environ.get("FITANALYTICS_GCS_BUCKET", "YOUR_BUCKET")
DATAPROC_CLUSTER = os.environ.get(
    "FITANALYTICS_DATAPROC_CLUSTER",
    "fitanalytics-cluster",
)
TEMP_BUCKET = os.environ.get(
    "FITANALYTICS_TEMP_BUCKET",
    GCS_BUCKET,
)

REPO_ROOT = os.environ.get(
    "FITANALYTICS_REPO_ROOT",
    "/home/airflow/gcs/data/gcp-retail-data-pipeline",
)
DATAFLOW_PY_FILE = os.path.join(
    REPO_ROOT,
    "dataflow",
    "fitanalytics_ingestion_pipeline.py",
)
SETUP_FILE = os.path.join(REPO_ROOT, "setup.py")
DBT_PROJECT_DIR = os.environ.get(
    "FITANALYTICS_DBT_PROJECT_DIR",
    os.path.join(REPO_ROOT, "dbt"),
)
DBT_PROFILES_DIR = os.environ.get(
    "FITANALYTICS_DBT_PROFILES_DIR",
    "/home/airflow/gcs/data/dbt_profiles",
)

SPARK_DRIVER_URI = os.environ.get(
    "FITANALYTICS_SPARK_DRIVER_URI",
    f"gs://{GCS_BUCKET}/code/spark_jobs/run_transformations.py",
)
SPARK_PYFILES_URI = os.environ.get(
    "FITANALYTICS_SPARK_PYFILES_URI",
    f"gs://{GCS_BUCKET}/code/spark_jobs/spark_jobs.zip",
)

BUSINESS_DATE = (
    "{{ dag_run.conf.get("
    "'business_date', "
    "data_interval_end.in_timezone('Asia/Kolkata').strftime('%Y-%m-%d')"
    ") }}"
)
BATCH_ID = (
    "{{ dag_run.conf.get("
    "'batch_id', "
    "data_interval_end.in_timezone('Asia/Kolkata').strftime('%Y%m%dT%H%M%S')"
    ") }}"
)

ENTITY_TABLES = {
    "customer": "stg_customer",
    "product": "stg_product",
    "purchase": "stg_purchase",
    "return": "stg_return",
    "recommendation": "stg_recommendation",
}

EXPECTED_HEADERS = {
    "customer": [
        "customer_id", "region", "height_cm", "weight_kg",
        "preferred_fit", "updated_at",
    ],
    "product": [
        "product_id", "sku", "brand", "category", "size_system",
        "size_label", "fit_profile", "unit_price", "updated_at",
    ],
    "purchase": [
        "purchase_id", "customer_id", "product_id", "recommended_size",
        "purchased_size", "quantity", "amount", "purchase_date", "updated_at",
    ],
    "return": [
        "return_id", "purchase_id", "customer_id", "product_id",
        "return_reason", "fit_feedback", "return_amount", "return_date",
    ],
    "recommendation": [
        "recommendation_id", "customer_id", "product_id", "recommended_size",
        "confidence_score", "recommendation_ts", "event_id",
    ],
}


def validate_landing_files(entity, business_date):
    """Validate file naming, extension, non-zero size and CSV header."""
    prefix = f"landing/{entity}/business_date={business_date}/"
    client = storage.Client(project=PROJECT_ID)
    blobs = [
        blob for blob in client.list_blobs(GCS_BUCKET, prefix=prefix)
        if blob.name and not blob.name.endswith("/")
    ]

    if not blobs:
        raise ValueError(f"No objects found under gs://{GCS_BUCKET}/{prefix}")

    expected_header = EXPECTED_HEADERS[entity]
    valid_csv_count = 0

    for blob in blobs:
        file_name = os.path.basename(blob.name)

        if not (file_name.startswith(f"{entity}_") and file_name.endswith(".csv")):
            raise ValueError(
                f"Unexpected file name for {entity}: {blob.name}. "
                f"Expected {entity}_*.csv"
            )

        blob.reload()
        if not blob.size or blob.size <= 0:
            raise ValueError(f"Empty file is not allowed: {blob.name}")

        preview = blob.download_as_text(start=0, end=8191)
        first_line = preview.splitlines()[0].lstrip("\ufeff")
        actual_header = next(csv.reader([first_line]))

        if actual_header != expected_header:
            raise ValueError(
                f"Header mismatch in {blob.name}. "
                f"Expected {expected_header}, got {actual_header}"
            )

        valid_csv_count += 1

    if valid_csv_count == 0:
        raise ValueError(f"No valid CSV files found for entity={entity}")


def failure_callback(context):
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    logging.error(
        "Fit Analytics pipeline failure. dag_id=%s task_id=%s run_id=%s",
        context.get("dag").dag_id if context.get("dag") else None,
        task_instance.task_id if task_instance else None,
        dag_run.run_id if dag_run else None,
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}


with DAG(
    dag_id="fitanalytics_batch_pipeline",
    description="End-to-end Fit Analytics batch pipeline on GCP",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="0 8,13 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "fit-analytics", "dataflow", "dataproc", "bigquery", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")
    stage_validation_gate = EmptyOperator(task_id="stage_validation_gate")
    pipeline_success = EmptyOperator(task_id="pipeline_success")

    stage_checks = []

    for entity, stage_table in ENTITY_TABLES.items():
        with TaskGroup(group_id=f"{entity}_ingestion") as ingestion_group:
            wait_for_file = GCSObjectsWithPrefixExistenceSensor(
                task_id="wait_for_file",
                bucket=GCS_BUCKET,
                prefix=(
                    f"landing/{entity}/business_date={BUSINESS_DATE}/"
                ),
                poke_interval=60,
                timeout=60 * 60,
                mode="reschedule",
                deferrable=True,
            )

            validate_files = PythonOperator(
                task_id="validate_files",
                python_callable=validate_landing_files,
                op_kwargs={
                    "entity": entity,
                    "business_date": BUSINESS_DATE,
                },
            )

            prepare_batch = BigQueryInsertJobOperator(
                task_id="prepare_batch",
                configuration={
                    "query": {
                        "query": f"""
                            DELETE FROM `{PROJECT_ID}.fitanalytics_stage.{stage_table}`
                            WHERE batch_id = '{BATCH_ID}';

                            DELETE FROM `{PROJECT_ID}.fitanalytics_audit.quarantine_records`
                            WHERE batch_id = '{BATCH_ID}'
                              AND entity_type = '{entity}';

                            DELETE FROM `{PROJECT_ID}.fitanalytics_audit.pipeline_audit`
                            WHERE batch_id = '{BATCH_ID}'
                              AND pipeline_name = 'fitanalytics_{entity}_ingestion';
                        """,
                        "useLegacySql": False,
                    }
                },
                location=BQ_LOCATION,
                deferrable=True,
            )

            run_dataflow = BeamRunPythonPipelineOperator(
                task_id="run_dataflow",
                runner="DataflowRunner",
                py_file=DATAFLOW_PY_FILE,
                py_options=[],
                pipeline_options={
                    "entity_type": entity,
                    "input": (
                        f"gs://{GCS_BUCKET}/landing/{entity}/"
                        f"business_date={BUSINESS_DATE}/{entity}_*.csv"
                    ),
                    "output_table": (
                        f"{PROJECT_ID}:fitanalytics_stage.{stage_table}"
                    ),
                    "quarantine_table": (
                        f"{PROJECT_ID}:fitanalytics_audit.quarantine_records"
                    ),
                    "audit_table": (
                        f"{PROJECT_ID}:fitanalytics_audit.pipeline_audit"
                    ),
                    "batch_id": BATCH_ID,
                    "temp_location": (
                        f"gs://{GCS_BUCKET}/dataflow/temp"
                    ),
                    "staging_location": (
                        f"gs://{GCS_BUCKET}/dataflow/staging"
                    ),
                    "setup_file": SETUP_FILE,
                    "save_main_session": True,
                },
                py_requirements=[
                    "apache-beam[gcp]>=2.60.0,<3.0.0",
                ],
                py_interpreter="python3",
                py_system_site_packages=False,
                dataflow_config=DataflowConfiguration(
                    job_name=f"fitanalytics-{entity}-batch",
                    project_id=PROJECT_ID,
                    location=REGION,
                    append_job_name=True,
                    poll_sleep=20,
                ),
                deferrable=True,
            )

            validate_stage = BigQueryCheckOperator(
                task_id="validate_stage",
                sql=f"""
                    SELECT
                      source_count = stage_count + rejected_count
                      AND status = 'PROCESSED'
                    FROM `{PROJECT_ID}.fitanalytics_audit.pipeline_audit`
                    WHERE pipeline_name = 'fitanalytics_{entity}_ingestion'
                      AND batch_id = '{BATCH_ID}'
                    ORDER BY end_time DESC
                    LIMIT 1
                """,
                use_legacy_sql=False,
                location=BQ_LOCATION,
                deferrable=True,
            )

            wait_for_file >> validate_files >> prepare_batch >> run_dataflow >> validate_stage

        start >> ingestion_group
        stage_checks.append(validate_stage)

    for check in stage_checks:
        check >> stage_validation_gate

    dataproc_job = {
        "reference": {
            "project_id": PROJECT_ID,
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER,
        },
        "pyspark_job": {
            "main_python_file_uri": SPARK_DRIVER_URI,
            "python_file_uris": [SPARK_PYFILES_URI],
            "args": [
                "--project_id",
                PROJECT_ID,
                "--temporary_gcs_bucket",
                TEMP_BUCKET,
                "--batch_id",
                BATCH_ID,
            ],
        },
    }

    run_dataproc_transformations = DataprocSubmitJobOperator(
        task_id="run_dataproc_transformations",
        job=dataproc_job,
        region=REGION,
        project_id=PROJECT_ID,
        deferrable=True,
    )

    validate_reconciliation = BigQueryCheckOperator(
        task_id="validate_reconciliation",
        sql=f"""
            SELECT
              COUNT(*) = 3
              AND COUNTIF(reconciliation_status = 'MISMATCH') = 0
            FROM `{PROJECT_ID}.fitanalytics_audit.reconciliation_audit`
            WHERE batch_id = '{BATCH_ID}'
              AND entity_type IN ('purchase', 'return', 'recommendation')
        """,
        use_legacy_sql=False,
        location=BQ_LOCATION,
        deferrable=True,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "set -euo pipefail; "
            f"cd '{DBT_PROJECT_DIR}'; "
            "dbt deps; "
            f"dbt build --profiles-dir '{DBT_PROFILES_DIR}'"
        ),
        env={
            "DBT_BIGQUERY_PROJECT": PROJECT_ID,
        },
        append_env=True,
    )

    archive_tasks = []

    for entity in ENTITY_TABLES:
        archive = GCSToGCSOperator(
            task_id=f"archive_{entity}_files",
            source_bucket=GCS_BUCKET,
            source_object=(
                f"landing/{entity}/business_date={BUSINESS_DATE}/"
            ),
            destination_bucket=GCS_BUCKET,
            destination_object=(
                f"archive/{entity}/business_date={BUSINESS_DATE}/"
                f"batch_id={BATCH_ID}/"
            ),
            move_object=True,
            replace=True,
            source_object_required=True,
            exact_match=False,
        )
        archive_tasks.append(archive)

    (
        stage_validation_gate
        >> run_dataproc_transformations
        >> validate_reconciliation
        >> run_dbt
    )

    for archive in archive_tasks:
        run_dbt >> archive >> pipeline_success
