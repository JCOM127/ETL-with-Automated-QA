"""Four post-load QA checks executed as PySpark DataFrame operations.

Each function returns an AuditResult dataclass so the reporting layer
can aggregate results without depending on check internals.
"""

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class AuditResult:
    """Container for a single QA check outcome.

    Attributes:
        check_name: Human-readable identifier for the check.
        passed: True only if the check found no issues.
        details: Dict of counts, samples, or delta values for the report.
    """

    check_name: str
    passed: bool
    details: dict


def check_completeness(
    customers_df: DataFrame,
    transactions_df: DataFrame,
) -> AuditResult:
    """Count NULL values in mandatory fields across both DataFrames.

    Args:
        customers_df: Spark DataFrame of customer records.
        transactions_df: Spark DataFrame of transaction records.

    Returns:
        AuditResult with passed=True only when all NULL counts are zero.
    """
    logger.debug("Running completeness check")
    cust_nulls = {
        col: customers_df.filter(F.col(col).isNull()).count()
        for col in ("name", "email", "country")
    }
    txn_nulls = {"amount": transactions_df.filter(F.col("amount").isNull()).count()}
    all_counts = {**cust_nulls, **txn_nulls}
    passed = all(v == 0 for v in all_counts.values())
    if not passed:
        logger.warning("Completeness check FAILED: null counts %s", all_counts)
    return AuditResult(
        check_name="completeness",
        passed=passed,
        details={"null_counts": all_counts},
    )


def check_uniqueness(customers_df: DataFrame) -> AuditResult:
    """Identify duplicate email values in the customers DataFrame.

    Args:
        customers_df: Spark DataFrame of customer records.

    Returns:
        AuditResult with passed=True only when no duplicate emails exist.
    """
    logger.debug("Running uniqueness check")
    duplicates = (
        customers_df.groupBy("email")
        .count()
        .filter(F.col("count") > 1)
        .select("email")
        .rdd.flatMap(lambda r: [r["email"]])
        .collect()
    )
    if duplicates:
        logger.warning("Uniqueness check FAILED: duplicate emails %s", duplicates)
    return AuditResult(
        check_name="uniqueness",
        passed=len(duplicates) == 0,
        details={"duplicate_emails": duplicates},
    )


def check_referential_integrity(
    transactions_df: DataFrame,
    customers_df: DataFrame,
) -> AuditResult:
    """Find transactions whose customer_id has no matching customer row.

    Args:
        transactions_df: Spark DataFrame of transaction records.
        customers_df: Spark DataFrame of customer records.

    Returns:
        AuditResult with passed=True only when no orphan transactions exist.
    """
    logger.debug("Running referential integrity check")
    customer_ids = customers_df.select("customer_id")
    orphans = (
        transactions_df.join(customer_ids, on="customer_id", how="left_anti")
        .select("customer_id")
        .distinct()
        .rdd.flatMap(lambda r: [r["customer_id"]])
        .collect()
    )
    if orphans:
        logger.warning(
            "Referential integrity check FAILED: orphan customer_ids %s", orphans
        )
    return AuditResult(
        check_name="referential_integrity",
        passed=len(orphans) == 0,
        details={"orphan_customer_ids": orphans},
    )


def _raw_totals_by_country(
    raw_customers: list[dict],
    raw_transactions: list[dict],
    spark: SparkSession,
) -> DataFrame:
    """Join raw lists and sum amount per country, excluding NULLs.

    Args:
        raw_customers: Unfiltered customer dicts (all original records).
        raw_transactions: Unfiltered transaction dicts (all original records).
        spark: Active SparkSession.

    Returns:
        DataFrame with columns (country, raw_total).
    """
    cust_df = spark.createDataFrame(raw_customers)
    txn_df = spark.createDataFrame(raw_transactions)
    return (
        txn_df.join(
            cust_df.select("customer_id", "country"), on="customer_id", how="left"
        )
        .filter(F.col("amount").isNotNull() & F.col("country").isNotNull())
        .groupBy("country")
        .agg(F.sum("amount").alias("raw_total"))
    )


def check_reconciliation(
    raw_customers: list[dict],
    raw_transactions: list[dict],
    aggregated_df: DataFrame,
) -> AuditResult:
    """Compare total amount per country between raw and cleaned data.

    Flags any country whose sum changed after the cleaning/join steps,
    exposing records that were silently dropped from the pipeline.

    Args:
        raw_customers: All original customer dicts (pre-gate).
        raw_transactions: All original transaction dicts (pre-gate).
        aggregated_df: Pipeline output with columns (country, total_amount).

    Returns:
        AuditResult with passed=True only when every country total matches.
    """
    logger.debug("Running reconciliation check")
    spark = SparkSession.builder.getOrCreate()
    raw_totals = _raw_totals_by_country(raw_customers, raw_transactions, spark)
    clean_totals = aggregated_df.withColumnRenamed("total_amount", "clean_total")
    comparison = raw_totals.join(clean_totals, on="country", how="full_outer").fillna(
        0.0
    )
    mismatches = [
        r.asDict()
        for r in comparison.filter(F.col("raw_total") != F.col("clean_total")).collect()
    ]
    if mismatches:
        logger.warning(
            "Reconciliation check FAILED: %d country mismatch(es) %s",
            len(mismatches),
            mismatches,
        )
    return AuditResult(
        check_name="reconciliation",
        passed=len(mismatches) == 0,
        details={"country_mismatches": mismatches},
    )
