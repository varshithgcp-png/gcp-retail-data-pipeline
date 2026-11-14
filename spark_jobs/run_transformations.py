"""Dataproc/PySpark driver for Fit Analytics Phase 3."""

import argparse
import logging

from google.cloud import bigquery
from pyspark.sql import SparkSession

from spark_jobs.bigquery_merges import (
    CUSTOMER_SCD2_SQL,
    PRODUCT_SCD2_SQL,
    PURCHASE_MERGE_SQL,
    RETURN_MERGE_SQL,
    RECOMMENDATION_MERGE_SQL,
)
from spark_jobs.common import read_bigquery, write_bigquery
from spark_jobs.dimension_transformations import (
    prepare_customer_dimension,
    prepare_product_dimension,
)
from spark_jobs.fact_transformations import (
    prepare_purchase,
    prepare_return,
    prepare_recommendation,
)


def bq_table(project_id, dataset, name):
    return f"{project_id}.{dataset}.{name}"


def run_query(client, sql_template, project_id, batch_id):
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)
        ]
    )
    client.query(
        sql_template.format(project=project_id),
        job_config=config,
    ).result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--temporary_gcs_bucket", required=True)
    parser.add_argument("--batch_id", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName(
        "fitanalytics-phase3-transformations"
    ).getOrCreate()
    client = bigquery.Client(project=args.project_id)

    customer_stage = read_bigquery(
        spark,
        bq_table(args.project_id, "fitanalytics_stage", "stg_customer"),
    )
    product_stage = read_bigquery(
        spark,
        bq_table(args.project_id, "fitanalytics_stage", "stg_product"),
    )

    customer_work = prepare_customer_dimension(customer_stage, args.batch_id)
    product_work = prepare_product_dimension(product_stage, args.batch_id)

    write_bigquery(
        customer_work,
        bq_table(args.project_id, "fitanalytics_history", "wrk_customer"),
        args.temporary_gcs_bucket,
    )
    write_bigquery(
        product_work,
        bq_table(args.project_id, "fitanalytics_history", "wrk_product"),
        args.temporary_gcs_bucket,
    )

    run_query(client, CUSTOMER_SCD2_SQL, args.project_id, args.batch_id)
    run_query(client, PRODUCT_SCD2_SQL, args.project_id, args.batch_id)

    customer_dim = read_bigquery(
        spark,
        bq_table(args.project_id, "fitanalytics_curated", "dim_customer"),
    )
    product_dim = read_bigquery(
        spark,
        bq_table(args.project_id, "fitanalytics_curated", "dim_product"),
    )

    jobs = [
        (
            "purchase", "stg_purchase", "wrk_purchase",
            prepare_purchase, PURCHASE_MERGE_SQL,
        ),
        (
            "return", "stg_return", "wrk_return",
            prepare_return, RETURN_MERGE_SQL,
        ),
        (
            "recommendation", "stg_recommendation", "wrk_recommendation",
            prepare_recommendation, RECOMMENDATION_MERGE_SQL,
        ),
    ]

    reject_table = bq_table(
        args.project_id,
        "fitanalytics_audit",
        "transformation_rejects",
    )

    for entity, stage_name, work_name, transform, merge_sql in jobs:
        logging.info("Processing entity=%s batch_id=%s", entity, args.batch_id)

        stage_df = read_bigquery(
            spark,
            bq_table(args.project_id, "fitanalytics_stage", stage_name),
        )

        valid_df, rejected_df = transform(
            stage_df,
            customer_dim,
            product_dim,
            args.batch_id,
        )

        write_bigquery(
            valid_df,
            bq_table(args.project_id, "fitanalytics_history", work_name),
            args.temporary_gcs_bucket,
        )

        if rejected_df.take(1):
            write_bigquery(
                rejected_df,
                reject_table,
                args.temporary_gcs_bucket,
            )

        run_query(client, merge_sql, args.project_id, args.batch_id)

    spark.stop()


if __name__ == "__main__":
    main()
