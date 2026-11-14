from datetime import date, datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from spark_jobs.common import add_sha256_hash, latest_per_key
from spark_jobs.fact_transformations import prepare_purchase, prepare_return


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("fitanalytics-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_latest_per_customer(spark):
    df = spark.createDataFrame(
        [
            ("C001", "REGULAR", "2026-11-01T08:00:00"),
            ("C001", "SLIM", "2026-11-02T08:00:00"),
        ],
        ["customer_id", "preferred_fit", "updated_at"],
    )

    result = latest_per_key(
        df,
        ["customer_id"],
        ["updated_at"],
    ).collect()

    assert len(result) == 1
    assert result[0]["preferred_fit"] == "SLIM"


def test_hash_changes_when_business_value_changes(spark):
    df = spark.createDataFrame(
        [
            ("C001", "REGULAR"),
            ("C001", "SLIM"),
        ],
        ["customer_id", "preferred_fit"],
    )

    rows = add_sha256_hash(
        df,
        ["preferred_fit"],
    ).select("record_hash").collect()

    assert rows[0]["record_hash"] != rows[1]["record_hash"]


def current_dimensions(spark):
    customer_dim = spark.createDataFrame(
        [
            ("C001", True),
            ("C002", True),
        ],
        ["customer_id", "is_current"],
    )
    product_dim = spark.createDataFrame(
        [
            ("P501", True),
        ],
        ["product_id", "is_current"],
    )
    return customer_dim, product_dim


def test_purchase_ri_path_does_not_require_return_columns(spark):
    customer_dim, product_dim = current_dimensions(spark)

    stage = spark.createDataFrame(
        [
            (
                "PU1",
                "C001",
                "P501",
                "M",
                "M",
                1,
                39.99,
                date(2026, 11, 1),
                datetime(2026, 11, 1, 10, 0, 0),
                datetime(2026, 11, 1, 10, 1, 0),
                "B1",
                "purchase_20261101.csv",
            )
        ],
        [
            "purchase_id",
            "customer_id",
            "product_id",
            "recommended_size",
            "purchased_size",
            "quantity",
            "amount",
            "purchase_date",
            "updated_at",
            "ingestion_ts",
            "batch_id",
            "source_file",
        ],
    )

    valid, rejected = prepare_purchase(
        stage,
        customer_dim,
        product_dim,
        "B1",
    )

    assert valid.count() == 1
    assert rejected.count() == 0


def test_return_rejects_unknown_purchase_id(spark):
    customer_dim, product_dim = current_dimensions(spark)

    stage = spark.createDataFrame(
        [
            (
                "R1",
                "PU9999",
                "C001",
                "P501",
                "OTHER",
                "TRUE_TO_SIZE",
                10.0,
                date(2026, 11, 7),
                datetime(2026, 11, 7, 10, 0, 0),
                "B1",
                "return_20261107.csv",
            )
        ],
        [
            "return_id",
            "purchase_id",
            "customer_id",
            "product_id",
            "return_reason",
            "fit_feedback",
            "return_amount",
            "return_date",
            "ingestion_ts",
            "batch_id",
            "source_file",
        ],
    )

    purchase_fact = spark.createDataFrame(
        [
            (
                "PU1",
                "C001",
                "P501",
                date(2026, 11, 1),
            )
        ],
        [
            "purchase_id",
            "customer_id",
            "product_id",
            "purchase_date",
        ],
    )

    valid, rejected = prepare_return(
        stage,
        customer_dim,
        product_dim,
        "B1",
        purchase_fact=purchase_fact,
    )

    assert valid.count() == 0
    rejected_rows = rejected.collect()
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["error_message"] == "Unknown purchase_id"


def test_return_rejects_purchase_customer_mismatch(spark):
    customer_dim, product_dim = current_dimensions(spark)

    stage = spark.createDataFrame(
        [
            (
                "R2",
                "PU1",
                "C002",
                "P501",
                "OTHER",
                "TRUE_TO_SIZE",
                10.0,
                date(2026, 11, 7),
                datetime(2026, 11, 7, 10, 0, 0),
                "B1",
                "return_20261107.csv",
            )
        ],
        [
            "return_id",
            "purchase_id",
            "customer_id",
            "product_id",
            "return_reason",
            "fit_feedback",
            "return_amount",
            "return_date",
            "ingestion_ts",
            "batch_id",
            "source_file",
        ],
    )

    purchase_fact = spark.createDataFrame(
        [
            (
                "PU1",
                "C001",
                "P501",
                date(2026, 11, 1),
            )
        ],
        [
            "purchase_id",
            "customer_id",
            "product_id",
            "purchase_date",
        ],
    )

    valid, rejected = prepare_return(
        stage,
        customer_dim,
        product_dim,
        "B1",
        purchase_fact=purchase_fact,
    )

    assert valid.count() == 0
    rejected_rows = rejected.collect()
    assert len(rejected_rows) == 1
    assert (
        rejected_rows[0]["error_message"]
        == "Return customer_id does not match purchase"
    )
