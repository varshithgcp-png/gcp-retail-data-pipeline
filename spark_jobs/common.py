"""Common PySpark helpers for Fit Analytics transformations."""

from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def read_bigquery(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read.format("bigquery")
        .option("table", table)
        .load()
    )


def write_bigquery(
    df: DataFrame,
    table: str,
    temporary_gcs_bucket: str,
    mode: str = "append",
) -> None:
    (
        df.write.format("bigquery")
        .option("table", table)
        .option("temporaryGcsBucket", temporary_gcs_bucket)
        .mode(mode)
        .save()
    )


def latest_per_key(
    df: DataFrame,
    key_columns: Sequence[str],
    order_columns: Sequence[str],
) -> DataFrame:
    """Keep the latest row for each business key."""
    ordering = [F.col(column).desc_nulls_last() for column in order_columns]
    window = Window.partitionBy(*key_columns).orderBy(*ordering)

    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def add_sha256_hash(
    df: DataFrame,
    business_columns: Iterable[str],
    output_column: str = "record_hash",
) -> DataFrame:
    """Create deterministic SHA-256 hash for change detection."""
    normalized = [
        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
        for column in business_columns
    ]
    return df.withColumn(
        output_column,
        F.sha2(F.concat_ws("||", *normalized), 256),
    )


def batch_rows(df: DataFrame, batch_id: str) -> DataFrame:
    return df.filter(F.col("batch_id") == F.lit(batch_id))
