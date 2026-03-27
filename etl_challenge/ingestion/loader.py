"""Ingestion layer: validate raw records through Pydantic contracts.

Records that pass are forwarded to the Spark layer; records that fail
are collected with their rejection reasons for the audit report.
"""

import logging
from dataclasses import dataclass, field

from pydantic import ValidationError

logger = logging.getLogger(__name__)

from etl_challenge.contracts.customer import Customer
from etl_challenge.contracts.transaction import Transaction


@dataclass
class IngestionResult:
    """Container returned by load_and_validate.

    Attributes:
        clean_customers: Records that passed the Customer contract.
        clean_transactions: Records that passed the Transaction contract.
        rejected_customers: Records that failed, each with a
            'rejection_reason' key added.
        rejected_transactions: Records that failed, each with a
            'rejection_reason' key added.
    """

    clean_customers: list[dict] = field(default_factory=list)
    clean_transactions: list[dict] = field(default_factory=list)
    rejected_customers: list[dict] = field(default_factory=list)
    rejected_transactions: list[dict] = field(default_factory=list)


def _validate_records(raw: list[dict], model) -> tuple[list[dict], list[dict]]:
    """Run each raw dict through a Pydantic model, splitting clean/rejected.

    Args:
        raw: List of raw record dicts to validate.
        model: A Pydantic BaseModel class used as the data contract.

    Returns:
        A tuple of (clean_records, rejected_records).  Rejected records
        carry an extra 'rejection_reason' key with the validation message.
    """
    clean: list[dict] = []
    rejected: list[dict] = []
    for record in raw:
        try:
            model.model_validate(record)
            clean.append(record)
        except ValidationError as exc:
            rejected.append({**record, "rejection_reason": str(exc)})
    return clean, rejected


def load_and_validate(
    raw_customers: list[dict],
    raw_transactions: list[dict],
) -> IngestionResult:
    """Gate raw records through Pydantic contracts and return split results.

    Args:
        raw_customers: Unvalidated customer dicts from the source.
        raw_transactions: Unvalidated transaction dicts from the source.

    Returns:
        An IngestionResult with clean and rejected lists for each entity.
    """
    clean_c, rejected_c = _validate_records(raw_customers, Customer)
    clean_t, rejected_t = _validate_records(raw_transactions, Transaction)
    logger.info(
        "Customers: %d clean, %d rejected", len(clean_c), len(rejected_c)
    )
    logger.info(
        "Transactions: %d clean, %d rejected", len(clean_t), len(rejected_t)
    )
    return IngestionResult(
        clean_customers=clean_c,
        clean_transactions=clean_t,
        rejected_customers=rejected_c,
        rejected_transactions=rejected_t,
    )
