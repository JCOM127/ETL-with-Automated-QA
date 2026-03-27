# ETL Pipeline with Automated QA

A PySpark + Pydantic pipeline that validates, transforms, and audits customer and
transaction data, then writes a JSON report telling you exactly what broke and why.

---

## What it does

1. **Rejects bad records at the door,** a Pydantic gate checks every raw record before
   it touches Spark. NULL emails, empty names, negative amounts: rejected immediately
   with a reason attached.
2. **Runs four data quality checks on what survives,** completeness, uniqueness,
   referential integrity, and a per-country amount reconciliation between raw and clean.
3. **Writes `audit_report.json`,** a timestamped JSON file with every check result,
   rejection count, and the overall pass/fail verdict.
4. **Fires mocked alerts,** webhook and email dispatchers with production-ready
   signatures, ready to wire to PagerDuty or SendGrid.

> **Why PySpark?** Changing one line, the Spark master URL, moves this pipeline from
> your laptop to AWS EMR, Databricks, or Google Dataproc unchanged. That is the
> scalability argument. No rewrite, no re-architecture.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11+ | |
| Java | 17 | Required by PySpark, see Docker below to skip this |
| Docker | any recent | Recommended: avoids Java setup entirely |

---

## Quickstart: Docker (recommended)

No Java, no virtualenv, no surprises.

```bash
# Build once
docker compose build

# Run the full test suite
docker compose run test

# Run integration tests only, produces audit_report.json in this folder
docker compose run integration

# Open the notebook — token required, change before sharing
JUPYTER_TOKEN=mysecret docker compose up notebook
# Then open: http://localhost:8888?token=mysecret
```

---

## Quickstart: Local

```bash
# Install package + dev dependencies
pip install -e ".[dev]"

# Run tests
pytest -m unit          # fast, no Spark warmup
pytest -m integration   # runs the full pipeline, writes audit_report.json
pytest -m functional    # verifies alert dispatchers are called correctly

# Open the notebook
jupyter notebook notebooks/poc.ipynb
```

---

## What you'll see

After `pytest -m integration`, open `audit_report.json`:

```json
{
  "timestamp": "...",
  "overall_pass": false,
  "rejected_at_gate": { "customers": 3, "transactions": 1 },
  "audit_checks": [
    { "check_name": "completeness",          "passed": true,  "details": {...} },
    { "check_name": "uniqueness",            "passed": false, "details": {...} },
    { "check_name": "referential_integrity", "passed": false, "details": {...} },
    { "check_name": "reconciliation",        "passed": false, "details": {...} }
  ]
}
```

Three of four checks fail, by design. The mock data contains NULL fields, duplicate
emails, an orphan transaction, and a country whose totals change after cleaning. Every
check is supposed to catch something real.

---

## Project layout

```
etl_challenge/
├── etl_challenge/
│   ├── data/mock_data.py       intentionally dirty mock datasets
│   ├── contracts/              Pydantic models, entrance gate (Layer 1 QA)
│   ├── ingestion/loader.py     splits records into clean / rejected
│   ├── transforms/pipeline.py  PySpark join + country aggregation
│   ├── audit/checks.py         four QA checks returning AuditResult
│   ├── alerts/dispatcher.py    send_webhook / send_email (mocked bodies)
│   └── reporting/report.py     build_report / save_report
├── tests/
│   ├── unit/                   28 tests total, fast, isolated
│   ├── integration/            full pipeline wired end-to-end
│   └── functional/             alert dispatcher call assertions
├── notebooks/poc.ipynb         narrative walkthrough, runs top to bottom
├── Dockerfile / docker-compose.yml
└── .github/workflows/ci.yml    lint, test, deploy (mocked)
```

---

## CI

GitHub Actions runs three jobs in sequence on every push:

```
lint   ->  ruff check + ruff format --check
test   ->  pytest tests/ -v --tb=short
deploy ->  echo (mocked, no live target)
```

---

## Logging

The pipeline uses Python's stdlib `logging` module — no extra dependencies. Each module
owns a `logger = logging.getLogger(__name__)`. Levels in use:

| Level | Where | What it says |
|-------|-------|--------------|
| `DEBUG` | `loader.py` | Every record accepted or rejected, full content |
| `INFO` | `loader.py` | Summary counts after validation |
| `INFO` | `reporting.py` | Report path and overall pass/fail on save |
| `WARNING` | `checks.py` | Each check that returns `passed=False`, with details |
| `ERROR` | `reporting.py` | If `audit_report.json` cannot be written |

To activate logging locally:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

> **PII warning:** `DEBUG` logs in `loader.py` print full records including names and
> emails. Do not ship DEBUG-level logs to an external aggregator (Datadog, CloudWatch)
> without first masking or hashing PII fields. `WARNING` logs in `checks.py` also
> include duplicate email values. In production, replace raw values with hashed or
> tokenised identifiers before any log is written.

---

## Scalability note on the entrance gate

The Pydantic gate iterates records in a Python loop, which is intentional for this
scope. In production, data arrives via `spark.read.parquet()` or `spark.read.jdbc()`
directly into Spark, bypassing the Python loop entirely. Record-level validation at
that scale moves into Spark UDFs or schema enforcement at the source (Avro/Parquet
schema, Kafka schema registry). The gate concept and architecture stay the same,
only the execution layer changes.

---

## Extending

**New QA check:**
1. Add a function to [`audit/checks.py`](etl_challenge/audit/checks.py) returning `AuditResult`
2. Add a unit test (pass + fail case) to [`tests/unit/test_checks.py`](tests/unit/test_checks.py)
3. Add it to the results list in [`tests/integration/test_pipeline.py`](tests/integration/test_pipeline.py)

**New alert channel:**
1. Add a function to [`alerts/dispatcher.py`](etl_challenge/alerts/dispatcher.py)
2. Add a functional test to [`tests/functional/test_alerts.py`](tests/functional/test_alerts.py) using `unittest.mock.patch`
