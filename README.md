# ETL Challenge with Automated QA

A production-pattern ETL pipeline built with PySpark and Pydantic, demonstrating
two-layer data quality enforcement, structured audit reporting, and mocked alerting.

## Why PySpark?

PySpark is chosen deliberately for **horizontal scalability**.  The same pipeline
code runs unchanged on a local laptop (`local[*]`), an AWS EMR cluster, Databricks,
or Google Dataproc — the only change is one configuration line: the Spark master URL.

## Directory Structure

```
etl_challenge/
├── etl_challenge/          # Main package
│   ├── data/               # Hardcoded mock datasets
│   ├── contracts/          # Pydantic entrance gate (Layer 1 QA)
│   ├── ingestion/          # Record loader — splits clean vs. rejected
│   ├── transforms/         # PySpark join + aggregation (Layer 2)
│   ├── audit/              # 4 post-load QA checks (Layer 2 QA)
│   ├── alerts/             # Webhook and email dispatchers (mocked)
│   └── reporting/          # JSON audit report builder
├── tests/
│   ├── unit/               # Fast isolated tests
│   ├── integration/        # Multi-module wiring tests
│   └── functional/         # End-to-end alert tests
├── notebooks/poc.ipynb     # Narrative walkthrough
└── .github/workflows/      # CI: lint → test → deploy
```

## Setup

```bash
pip install -e ".[dev]"
```

## Run the Notebook

```bash
jupyter notebook notebooks/poc.ipynb
```

## Run Tests

```bash
# All tests
pytest tests/ -v

# By marker
pytest -m unit
pytest -m integration
pytest -m functional
```

## CI Pipeline

Three jobs run in sequence via GitHub Actions:

1. **lint** — `ruff check` + `ruff format --check`
2. **test** — `pytest tests/ -v --tb=short`
3. **deploy** — mocked echo step (no live target in this challenge)

## Extending the Pipeline

### Adding a New QA Check

1. Add a function to `etl_challenge/audit/checks.py` returning `AuditResult`.
2. Call it in `tests/integration/test_pipeline.py` and add unit tests in
   `tests/unit/test_checks.py`.
3. Pass the result into `build_report()` in your orchestration script.

### Adding a New Alert Channel

1. Add a new function to `etl_challenge/alerts/dispatcher.py` with a Google Style
   docstring describing its production behaviour.
2. Add a functional test in `tests/functional/test_alerts.py` using
   `unittest.mock.patch` to assert the function is called correctly.
