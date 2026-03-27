"""PySpark ETL pipeline: join customers to transactions and aggregate.

SparkSession is initialised once at module level.  Changing the master
URL from 'local[*]' to an EMR, Databricks, or Dataproc cluster URL is
the only change needed to run this pipeline at scale.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_spark = (
    SparkSession.builder.master("local[*]")
    .appName("etl_challenge")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)


def run_pipeline(
    clean_customers: list[dict],
    clean_transactions: list[dict],
) -> tuple[DataFrame, DataFrame]:
    """Join transactions to customers and aggregate spend per country.

    Args:
        clean_customers: Validated customer records as plain dicts.
        clean_transactions: Validated transaction records as plain dicts.

    Returns:
        A tuple of two DataFrames:
            - joined_df: Transactions enriched with customer fields,
              duplicate customer_id column removed.
            - aggregated_df: Total amount per country, sorted descending
              by total_amount.
    """
    customers_df = _spark.createDataFrame(clean_customers)
    transactions_df = _spark.createDataFrame(clean_transactions)

    joined_df = transactions_df.join(customers_df, on="customer_id", how="inner")

    aggregated_df = (
        joined_df.groupBy("country")
        .agg(F.sum("amount").alias("total_amount"))
        .orderBy(F.col("total_amount").desc())
    )

    return joined_df, aggregated_df
