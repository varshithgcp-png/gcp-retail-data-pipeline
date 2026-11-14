import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from spark_jobs.common import add_sha256_hash, latest_per_key


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("fitanalytics-tests")
        .getOrCreate()
    )
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
