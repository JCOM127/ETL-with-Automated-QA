"""Pydantic v2 contract for raw customer records.

Acts as the entrance gate: any record that violates this schema is
rejected before it reaches the Spark layer.
"""

from pydantic import BaseModel, field_validator


class Customer(BaseModel):
    """Data contract for a single customer record.

    Attributes:
        customer_id: Unique integer identifier for the customer.
        name: Full name; must be a non-empty string.
        email: Contact email; must contain '@' and '.'.
        country: Country of residence; must be a non-empty string.
    """

    customer_id: int
    name: str
    email: str
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

    @field_validator("email")
    @classmethod
    def must_be_valid_email(cls, value: str) -> str:
        """Reject emails that do not contain '@' and '.'.

        Args:
            value: The email string to validate.

        Returns:
            The original value if it looks like a valid email.

        Raises:
            ValueError: If '@' or '.' are missing from the email.
        """
        if "@" not in value or "." not in value:
            raise ValueError(f"'{value}' is not a valid email address")
        return value
