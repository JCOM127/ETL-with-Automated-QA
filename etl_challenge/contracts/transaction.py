"""Pydantic v2 contract for raw transaction records.

Acts as the entrance gate: any record that violates this schema is
rejected before it reaches the Spark layer.
"""

from datetime import datetime

from pydantic import BaseModel, field_validator


class Transaction(BaseModel):
    """Data contract for a single transaction record.

    Attributes:
        transaction_id: Unique integer identifier for the transaction.
        customer_id: Positive integer referencing an existing customer.
        amount: Monetary value; must be strictly greater than zero.
        date: Transaction date string in YYYY-MM-DD format.
    """

    transaction_id: int
    customer_id: int
    amount: float
    date: str

    @field_validator("customer_id")
    @classmethod
    def must_be_positive(cls, value: int) -> int:
        """Reject non-positive customer_id values.

        Args:
            value: The customer_id integer to validate.

        Returns:
            The original value if it is positive.

        Raises:
            ValueError: If value is zero or negative.
        """
        if value <= 0:
            raise ValueError(f"customer_id must be positive, got {value}")
        return value

    @field_validator("amount")
    @classmethod
    def must_be_positive_amount(cls, value: float) -> float:
        """Reject zero or negative amounts.

        Args:
            value: The amount float to validate.

        Returns:
            The original value if it is strictly greater than zero.

        Raises:
            ValueError: If value is zero or negative.
        """
        if value <= 0:
            raise ValueError(f"amount must be > 0, got {value}")
        return value

    @field_validator("date")
    @classmethod
    def must_be_yyyy_mm_dd(cls, value: str) -> str:
        """Reject dates that do not conform to YYYY-MM-DD.

        Args:
            value: The date string to validate.

        Returns:
            The original value if parsing succeeds.

        Raises:
            ValueError: If the string cannot be parsed as YYYY-MM-DD.
        """
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"date '{value}' must be in YYYY-MM-DD format") from exc
        return value
