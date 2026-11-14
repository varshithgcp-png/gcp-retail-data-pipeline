"""BigQuery audit and batch-control helpers."""

from google.cloud import bigquery


def _query_config(batch_id, entity_type=None):
    params = [
        bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
    ]
    if entity_type is not None:
        params.append(
            bigquery.ScalarQueryParameter(
                "entity_type", "STRING", entity_type
            )
        )
    return bigquery.QueryJobConfig(query_parameters=params)


def clear_batch_rows(
    client,
    table,
    batch_id,
    entity_type=None,
):
    """Delete prior work/reject rows for a replayed batch."""
    predicate = "batch_id = @batch_id"
    if entity_type is not None:
        predicate += " AND entity_type = @entity_type"

    sql = f"DELETE FROM `{table}` WHERE {predicate}"
    client.query(
        sql,
        job_config=_query_config(batch_id, entity_type),
    ).result()


def record_fact_reconciliation(
    client,
    project_id,
    entity_type,
    batch_id,
    stage_table,
    work_table,
    target_table,
    business_key,
):
    """Reconcile stage -> deduped/rejected -> target for one batch."""

    cleanup_sql = f"""
    DELETE FROM `{project_id}.fitanalytics_audit.reconciliation_audit`
    WHERE batch_id = @batch_id
      AND entity_type = @entity_type
    """
    config = _query_config(batch_id, entity_type)
    client.query(cleanup_sql, job_config=config).result()

    sql = f"""
    INSERT INTO `{project_id}.fitanalytics_audit.reconciliation_audit`
    (
      pipeline_name,
      entity_type,
      batch_id,
      stage_count,
      valid_count,
      rejected_count,
      duplicate_count,
      target_count,
      reconciliation_status,
      audit_ts
    )
    WITH counts AS (
      SELECT
        (
          SELECT COUNT(*)
          FROM `{stage_table}`
          WHERE batch_id = @batch_id
        ) AS stage_count,
        (
          SELECT COUNT(*)
          FROM `{work_table}`
          WHERE batch_id = @batch_id
        ) AS valid_count,
        (
          SELECT COUNT(*)
          FROM `{project_id}.fitanalytics_audit.transformation_rejects`
          WHERE batch_id = @batch_id
            AND entity_type = @entity_type
        ) AS rejected_count,
        (
          SELECT COUNT(*)
          FROM `{work_table}` w
          JOIN `{target_table}` t
            ON t.{business_key} = w.{business_key}
          WHERE w.batch_id = @batch_id
        ) AS target_count
    )
    SELECT
      'fitanalytics_dataproc_transform',
      @entity_type,
      @batch_id,
      stage_count,
      valid_count,
      rejected_count,
      stage_count - valid_count - rejected_count AS duplicate_count,
      target_count,
      CASE
        WHEN stage_count - valid_count - rejected_count >= 0
         AND valid_count = target_count
        THEN 'MATCHED'
        ELSE 'MISMATCH'
      END,
      CURRENT_TIMESTAMP()
    FROM counts
    """

    client.query(sql, job_config=config).result()


def update_watermark(
    client,
    project_id,
    entity_type,
    batch_id,
    source_table,
    timestamp_column,
):
    sql = f"""
    MERGE `{project_id}.fitanalytics_audit.watermark_control` t
    USING (
      SELECT
        'fitanalytics_dataproc_transform' AS pipeline_name,
        @entity_type AS entity_type,
        MAX({timestamp_column}) AS last_processed_ts,
        @batch_id AS last_batch_id
      FROM `{source_table}`
      WHERE batch_id = @batch_id
    ) s
    ON t.pipeline_name = s.pipeline_name
       AND t.entity_type = s.entity_type
    WHEN MATCHED AND s.last_processed_ts IS NOT NULL THEN
      UPDATE SET
        last_processed_ts = GREATEST(
          COALESCE(t.last_processed_ts, TIMESTAMP('1970-01-01')),
          s.last_processed_ts
        ),
        last_batch_id = s.last_batch_id,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND s.last_processed_ts IS NOT NULL THEN
      INSERT (
        pipeline_name, entity_type, last_processed_ts,
        last_batch_id, updated_at
      )
      VALUES (
        s.pipeline_name, s.entity_type, s.last_processed_ts,
        s.last_batch_id, CURRENT_TIMESTAMP()
      )
    """

    client.query(
        sql,
        job_config=_query_config(batch_id, entity_type),
    ).result()
