"""Unit tests for the four post-load audit checks."""

import pytest

from etl_challenge.audit.checks import (
    check_completeness,
    check_reconciliation,
    check_referential_integrity,
    check_uniqueness,
)


@pytest.mark.unit
def test_completeness_clean_passes(
    sample_clean_customers_df, sample_clean_transactions_df
):
    """Completeness check on clean data must pass."""
    result = check_completeness(sample_clean_customers_df, sample_clean_transactions_df)
    assert result.passed is True


@pytest.mark.unit
def test_completeness_dirty_fails(
    sample_dirty_customers_df, sample_dirty_transactions_df
):
    """Completeness check on dirty data must fail."""
    result = check_completeness(sample_dirty_customers_df, sample_dirty_transactions_df)
    assert result.passed is False


@pytest.mark.unit
def test_uniqueness_clean_passes(sample_clean_customers_df):
    """Uniqueness check on clean data must pass."""
    result = check_uniqueness(sample_clean_customers_df)
    assert result.passed is True


@pytest.mark.unit
def test_uniqueness_dirty_fails(sample_dirty_customers_df):
    """Uniqueness check on data with duplicate email must fail."""
    result = check_uniqueness(sample_dirty_customers_df)
    assert result.passed is False
    assert "ana@email.com" in result.details["duplicate_emails"]


@pytest.mark.unit
def test_referential_integrity_clean_passes(
    sample_clean_transactions_df, sample_clean_customers_df
):
    """Referential integrity check on clean data must pass."""
    result = check_referential_integrity(
        sample_clean_transactions_df, sample_clean_customers_df
    )
    assert result.passed is True


@pytest.mark.unit
def test_referential_integrity_dirty_fails(
    sample_dirty_transactions_df, sample_clean_customers_df
):
    """Referential integrity check must fail when orphan customer_id exists."""
    result = check_referential_integrity(
        sample_dirty_transactions_df, sample_clean_customers_df
    )
    assert result.passed is False
    assert 99 in result.details["orphan_customer_ids"]


@pytest.mark.unit
def test_reconciliation_totals_match_passes(spark):
    """Reconciliation passes when raw and clean per-country totals are identical."""
    raw_customers = [
        {"customer_id": 1, "name": "Ana", "email": "ana@x.com", "country": "Colombia"}
    ]
    raw_transactions = [
        {"transaction_id": 100, "customer_id": 1, "amount": 200.0, "date": "2025-01-01"}
    ]
    aggregated_df = spark.createDataFrame(
        [("Colombia", 200.0)], ["country", "total_amount"]
    )
    result = check_reconciliation(raw_customers, raw_transactions, aggregated_df)
    assert result.passed is True


@pytest.mark.unit
def test_reconciliation_totals_differ_fails(spark):
    """Reconciliation fails when a country total changes after cleaning."""
    raw_customers = [
        {"customer_id": 1, "name": "Ana", "email": "ana@x.com", "country": "Colombia"},
        {"customer_id": 2, "name": "Juan", "email": "juan@x.com", "country": "Mexico"},
    ]
    raw_transactions = [
        {
            "transaction_id": 100,
            "customer_id": 1,
            "amount": 200.0,
            "date": "2025-01-01",
        },
        {
            "transaction_id": 101,
            "customer_id": 2,
            "amount": 150.0,
            "date": "2025-01-02",
        },
    ]
    # Mexico was cleaned out — aggregated only shows Colombia
    aggregated_df = spark.createDataFrame(
        [("Colombia", 200.0)], ["country", "total_amount"]
    )
    result = check_reconciliation(raw_customers, raw_transactions, aggregated_df)
    assert result.passed is False
    countries = [m["country"] for m in result.details["country_mismatches"]]
    assert "Mexico" in countries
