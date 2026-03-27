"""Hardcoded mock datasets used throughout the ETL pipeline.

These records are intentionally dirty so that every QA check has at
least one failing record to surface.
"""

CUSTOMERS_RAW = [
    {"customer_id": 1, "name": "Ana Torres", "email": "ana@email.com", "country": "Colombia"},
    {"customer_id": 2, "name": "Juan Pérez", "email": None, "country": "Mexico"},
    {"customer_id": 3, "name": "Laura Gómez", "email": "laura_gomez@email.com", "country": None},
    {"customer_id": 4, "name": "Juan Pérez", "email": "juanperez@email.com", "country": "Mexico"},
    {"customer_id": 5, "name": None, "email": "andres@email.com", "country": "Chile"},
]

TRANSACTIONS_RAW = [
    {"transaction_id": 100, "customer_id": 1, "amount": 200.0, "date": "2025-01-01"},
    {"transaction_id": 101, "customer_id": 2, "amount": 150.0, "date": "2025-01-02"},
    {"transaction_id": 102, "customer_id": 2, "amount": 150.0, "date": "2025-01-02"},  # exact duplicate of 101
    {"transaction_id": 103, "customer_id": 3, "amount": None, "date": "2025-01-03"},  # NULL amount
    {"transaction_id": 104, "customer_id": 6, "amount": 300.0, "date": "2025-01-04"},  # customer_id 6 does not exist
]
