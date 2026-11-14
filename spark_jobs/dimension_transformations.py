"""Prepare customer and product SCD2 work records."""

from pyspark.sql import DataFrame

from spark_jobs.common import add_sha256_hash, batch_rows, latest_per_key


CUSTOMER_HASH_COLUMNS = [
    "region",
    "height_cm",
    "weight_kg",
    "preferred_fit",
]

PRODUCT_HASH_COLUMNS = [
    "sku",
    "brand",
    "category",
    "size_system",
    "size_label",
    "fit_profile",
    "unit_price",
]


def prepare_customer_dimension(stage_df: DataFrame, batch_id: str) -> DataFrame:
    batch = batch_rows(stage_df, batch_id)

    deduped = latest_per_key(
        batch,
        key_columns=["customer_id"],
        order_columns=["updated_at", "ingestion_ts"],
    )

    return add_sha256_hash(deduped, CUSTOMER_HASH_COLUMNS).select(
        "customer_id",
        "region",
        "height_cm",
        "weight_kg",
        "preferred_fit",
        "updated_at",
        "record_hash",
        "batch_id",
        "source_file",
        "ingestion_ts",
    )


def prepare_product_dimension(stage_df: DataFrame, batch_id: str) -> DataFrame:
    batch = batch_rows(stage_df, batch_id)

    deduped = latest_per_key(
        batch,
        key_columns=["product_id"],
        order_columns=["updated_at", "ingestion_ts"],
    )

    return add_sha256_hash(deduped, PRODUCT_HASH_COLUMNS).select(
        "product_id",
        "sku",
        "brand",
        "category",
        "size_system",
        "size_label",
        "fit_profile",
        "unit_price",
        "updated_at",
        "record_hash",
        "batch_id",
        "source_file",
        "ingestion_ts",
    )
