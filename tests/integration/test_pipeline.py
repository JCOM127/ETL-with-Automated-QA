"""Integration tests: wire loader, pipeline, and audit checks together."""

import pytest

from etl_challenge.audit.checks import (
    check_completeness,
    check_reconciliation,
    check_referential_integrity,
    check_uniqueness,
)
from etl_challenge.data.mock_data import CUSTOMERS_RAW, TRANSACTIONS_RAW
from etl_challenge.ingestion.loader import load_and_validate
from etl_challenge.reporting.report import build_report, save_report
from etl_challenge.transforms.pipeline import run_pipeline


@pytest.mark.integration
def test_load_and_validate_rejects_dirty_records():
    """Loader must reject at least one customer and one transaction."""
    result = load_and_validate(CUSTOMERS_RAW, TRANSACTIONS_RAW)
    assert len(result.rejected_customers) >= 1
    assert len(result.rejected_transactions) >= 1


@pytest.mark.integration
def test_run_pipeline_joined_df_nonempty():
    """Pipeline join on clean loader output must return a non-empty DataFrame."""
    result = load_and_validate(CUSTOMERS_RAW, TRANSACTIONS_RAW)
    joined_df, _ = run_pipeline(result.clean_customers, result.clean_transactions)
    assert joined_df.count() > 0


@pytest.mark.integration
def test_run_pipeline_aggregated_has_total_amount_column():
    """Aggregated DataFrame must contain a 'total_amount' column."""
    result = load_and_validate(CUSTOMERS_RAW, TRANSACTIONS_RAW)
    _, aggregated_df = run_pipeline(result.clean_customers, result.clean_transactions)
    assert "total_amount" in aggregated_df.columns


@pytest.mark.integration
def test_all_audit_checks_produce_at_least_one_failure():
    """Running all 4 checks on pipeline output must yield at least one failure.

    The JSON audit report is always saved to audit_report.json so failures
    are visible without re-running the pipeline.
    """
    from pyspark.sql import SparkSession

    ingestion = load_and_validate(CUSTOMERS_RAW, TRANSACTIONS_RAW)
    joined_df, aggregated_df = run_pipeline(
        ingestion.clean_customers, ingestion.clean_transactions
    )

    spark = SparkSession.builder.getOrCreate()
    customers_df = spark.createDataFrame(ingestion.clean_customers)
    transactions_df = spark.createDataFrame(ingestion.clean_transactions)

    results = [
        check_completeness(customers_df, transactions_df),
        check_uniqueness(customers_df),
        check_referential_integrity(transactions_df, customers_df),
        check_reconciliation(CUSTOMERS_RAW, TRANSACTIONS_RAW, aggregated_df),
    ]

    rejected_counts = {
        "customers": len(ingestion.rejected_customers),
        "transactions": len(ingestion.rejected_transactions),
    }
    report = build_report(results, rejected_counts)
    save_report(report, "audit_report.json")

    assert any(not r.passed for r in results)
