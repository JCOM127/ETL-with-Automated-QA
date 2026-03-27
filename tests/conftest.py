"""Shared pytest fixtures: SparkSession and sample DataFrames."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from etl_challenge.audit.checks import AuditResult


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession shared across the test session.

    Yields:
        A SparkSession configured for local mode with the UI disabled.
    """
    session = (
        SparkSession.builder.master("local[*]")
        .appName("etl_challenge_tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", StringType(), True),
    ]
)


@pytest.fixture
def sample_clean_customers_df(spark):
    """Return a DataFrame with two valid customer rows.

    Args:
        spark: Session-scoped SparkSession fixture.

    Returns:
        Spark DataFrame with two clean customer records.
    """
    data = [
        (1, "Ana Torres", "ana@email.com", "Colombia"),
        (4, "Juan Pérez", "juanperez@email.com", "Mexico"),
    ]
    return spark.createDataFrame(data, schema=_CUSTOMER_SCHEMA)


@pytest.fixture
def sample_dirty_customers_df(spark):
    """Return a DataFrame with a NULL name and a duplicate email.

    Args:
        spark: Session-scoped SparkSession fixture.

    Returns:
        Spark DataFrame containing intentionally dirty customer records.
    """
    data = [
        (1, "Ana Torres", "ana@email.com", "Colombia"),
        (2, None, "ana@email.com", "Mexico"),  # NULL name, duplicate email
    ]
    return spark.createDataFrame(data, schema=_CUSTOMER_SCHEMA)


@pytest.fixture
def sample_clean_transactions_df(spark):
    """Return a DataFrame with two valid transaction rows.

    Args:
        spark: Session-scoped SparkSession fixture.

    Returns:
        Spark DataFrame with two clean transaction records.
    """
    data = [
        (100, 1, 200.0, "2025-01-01"),
        (101, 4, 150.0, "2025-01-02"),
    ]
    return spark.createDataFrame(data, schema=_TRANSACTION_SCHEMA)


@pytest.fixture
def sample_dirty_transactions_df(spark):
    """Return a DataFrame with a NULL amount and an orphan customer_id.

    Args:
        spark: Session-scoped SparkSession fixture.

    Returns:
        Spark DataFrame containing intentionally dirty transaction records.
    """
    data = [
        (100, 1, 200.0, "2025-01-01"),
        (103, 3, None, "2025-01-03"),  # NULL amount
        (104, 99, 300.0, "2025-01-04"),  # customer_id 99 does not exist
    ]
    return spark.createDataFrame(data, schema=_TRANSACTION_SCHEMA)


@pytest.fixture
def sample_audit_results():
    """Return one passing and one failing AuditResult.

    Returns:
        List containing two AuditResult instances.
    """
    return [
        AuditResult(check_name="completeness", passed=True, details={}),
        AuditResult(
            check_name="uniqueness",
            passed=False,
            details={"duplicate_emails": ["x@y.com"]},
        ),
    ]
