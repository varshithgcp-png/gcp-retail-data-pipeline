"""Prepare Fit Analytics fact work records with dedupe and RI validation."""

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.common import add_sha256_hash, batch_rows, latest_per_key


def _split_referential_integrity(
    df: DataFrame,
    customer_dim: DataFrame,
    product_dim: DataFrame,
    entity_type: str,
    purchase_fact: DataFrame = None,
) -> Tuple[DataFrame, DataFrame]:
    customers = (
        customer_dim.filter(F.col("is_current") == True)
        .select("customer_id")
        .distinct()
        .withColumn("_customer_exists", F.lit(True))
    )
    products = (
        product_dim.filter(F.col("is_current") == True)
        .select("product_id")
        .distinct()
        .withColumn("_product_exists", F.lit(True))
    )

    checked = (
        df.join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
    )

    if purchase_fact is not None and "purchase_id" in df.columns:
        purchases = (
            purchase_fact.select("purchase_id")
            .distinct()
            .withColumn("_purchase_exists", F.lit(True))
        )
        checked = checked.join(purchases, "purchase_id", "left")
    else:
        checked = checked.withColumn("_purchase_exists", F.lit(True))

    checked = checked.withColumn(
        "_error_message",
        F.when(F.col("_customer_exists").isNull(), F.lit("Unknown customer_id"))
        .when(F.col("_product_exists").isNull(), F.lit("Unknown product_id"))
        .when(F.col("_purchase_exists").isNull(), F.lit("Unknown purchase_id"))
        .otherwise(F.lit(None)),
    )

    valid = (
        checked.filter(F.col("_error_message").isNull())
        .drop(
            "_customer_exists",
            "_product_exists",
            "_purchase_exists",
            "_error_message",
        )
    )

    raw_columns = [F.col(column) for column in df.columns]
    rejected = (
        checked.filter(F.col("_error_message").isNotNull())
        .select(
            F.lit(entity_type).alias("entity_type"),
            F.to_json(F.struct(*raw_columns)).alias("raw_record"),
            F.col("_error_message").alias("error_message"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("ingestion_ts"),
        )
    )

    return valid, rejected


def prepare_purchase(
    stage_df: DataFrame,
    customer_dim: DataFrame,
    product_dim: DataFrame,
    batch_id: str,
) -> Tuple[DataFrame, DataFrame]:
    batch = batch_rows(stage_df, batch_id)
    deduped = latest_per_key(
        batch,
        ["purchase_id"],
        ["updated_at", "ingestion_ts"],
    )

    hashed = add_sha256_hash(
        deduped,
        [
            "customer_id", "product_id", "recommended_size",
            "purchased_size", "quantity", "amount", "purchase_date",
        ],
    )

    valid, rejected = _split_referential_integrity(
        hashed, customer_dim, product_dim, "purchase"
    )

    return valid.select(
        "purchase_id", "customer_id", "product_id",
        "recommended_size", "purchased_size", "quantity", "amount",
        "purchase_date", "updated_at", "record_hash",
        "batch_id", "source_file", "ingestion_ts",
    ), rejected


def prepare_return(
    stage_df: DataFrame,
    customer_dim: DataFrame,
    product_dim: DataFrame,
    batch_id: str,
    purchase_fact: DataFrame = None,
) -> Tuple[DataFrame, DataFrame]:
    batch = batch_rows(stage_df, batch_id)
    deduped = latest_per_key(
        batch,
        ["return_id"],
        ["ingestion_ts"],
    )

    hashed = add_sha256_hash(
        deduped,
        [
            "purchase_id", "customer_id", "product_id",
            "return_reason", "fit_feedback", "return_amount", "return_date",
        ],
    )

    valid, rejected = _split_referential_integrity(
        hashed,
        customer_dim,
        product_dim,
        "return",
        purchase_fact=purchase_fact,
    )

    return valid.select(
        "return_id", "purchase_id", "customer_id", "product_id",
        "return_reason", "fit_feedback", "return_amount", "return_date",
        "record_hash", "batch_id", "source_file", "ingestion_ts",
    ), rejected


def prepare_recommendation(
    stage_df: DataFrame,
    customer_dim: DataFrame,
    product_dim: DataFrame,
    batch_id: str,
) -> Tuple[DataFrame, DataFrame]:
    batch = batch_rows(stage_df, batch_id)
    deduped = latest_per_key(
        batch,
        ["event_id"],
        ["recommendation_ts", "ingestion_ts"],
    )

    hashed = add_sha256_hash(
        deduped,
        [
            "recommendation_id", "customer_id", "product_id",
            "recommended_size", "confidence_score",
            "recommendation_ts", "event_id",
        ],
    )

    valid, rejected = _split_referential_integrity(
        hashed, customer_dim, product_dim, "recommendation"
    )

    return valid.select(
        "recommendation_id", "customer_id", "product_id",
        "recommended_size", "confidence_score", "recommendation_ts",
        "event_id", "record_hash", "batch_id", "source_file", "ingestion_ts",
    ), rejected
