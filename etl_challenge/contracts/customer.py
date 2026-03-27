"""Pydantic v2 contract for raw customer records.

Acts as the entrance gate: any record that violates this schema is
rejected before it reaches the Spark layer.
"""

from pydantic import BaseModel, EmailStr, field_validator


class Customer(BaseModel):
    """Data contract for a single customer record.

    Attributes:
        customer_id: Unique integer identifier for the customer.
        name: Full name; must be a non-empty string.
        email: Contact email; validated against RFC 5322 by pydantic[email].
        country: Country of residence; must be a non-empty string.
    """

    customer_id: int
    name: str
    email: EmailStr
    country: str

    @field_validator("name", "country")
    @classmethod
    def must_be_non_empty(cls, value: str, info) -> str:
        """Reject blank name or country strings.

        Args:
            value: The field value to validate.
            info: Pydantic validation info carrying the field name.

        Returns:
            The original value if it is non-empty.

        Raises:
            ValueError: If value is an empty or whitespace-only string.
        """
        if not value or not value.strip():
            raise ValueError(f"'{info.field_name}' must be a non-empty string")
        return value
