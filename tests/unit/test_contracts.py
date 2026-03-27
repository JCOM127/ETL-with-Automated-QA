"""Unit tests for Pydantic data contracts."""

import pytest
from pydantic import ValidationError

from etl_challenge.contracts.customer import Customer
from etl_challenge.contracts.transaction import Transaction


@pytest.mark.unit
def test_valid_customer_passes():
    """A fully populated customer dict must pass validation."""
    Customer.model_validate(
        {
            "customer_id": 1,
            "name": "Ana",
            "email": "ana@email.com",
            "country": "Colombia",
        }
    )


@pytest.mark.unit
def test_customer_email_none_raises():
    """email=None must raise ValidationError."""
    with pytest.raises(ValidationError):
        Customer.model_validate(
            {"customer_id": 1, "name": "Ana", "email": None, "country": "Colombia"}
        )


@pytest.mark.unit
def test_customer_name_empty_raises():
    """name='' must raise ValidationError."""
    with pytest.raises(ValidationError):
        Customer.model_validate(
            {
                "customer_id": 1,
                "name": "",
                "email": "ana@email.com",
                "country": "Colombia",
            }
        )


@pytest.mark.unit
def test_customer_country_none_raises():
    """country=None must raise ValidationError."""
    with pytest.raises(ValidationError):
        Customer.model_validate(
            {"customer_id": 1, "name": "Ana", "email": "ana@email.com", "country": None}
        )


@pytest.mark.unit
def test_valid_transaction_passes():
    """A fully valid transaction dict must pass validation."""
    Transaction.model_validate(
        {"transaction_id": 100, "customer_id": 1, "amount": 200.0, "date": "2025-01-01"}
    )


@pytest.mark.unit
def test_transaction_negative_amount_raises():
    """amount=-1 must raise ValidationError."""
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {
                "transaction_id": 100,
                "customer_id": 1,
                "amount": -1,
                "date": "2025-01-01",
            }
        )


@pytest.mark.unit
def test_transaction_none_amount_raises():
    """amount=None must raise ValidationError."""
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {
                "transaction_id": 100,
                "customer_id": 1,
                "amount": None,
                "date": "2025-01-01",
            }
        )


@pytest.mark.unit
def test_transaction_wrong_date_format_raises():
    """date in DD-MM-YYYY format must raise ValidationError."""
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {
                "transaction_id": 100,
                "customer_id": 1,
                "amount": 100.0,
                "date": "01-01-2025",
            }
        )
