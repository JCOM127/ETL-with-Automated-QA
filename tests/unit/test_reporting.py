"""Unit tests for the audit report builder and writer."""

import json
import tempfile

import pytest

from etl_challenge.audit.checks import AuditResult
from etl_challenge.reporting.report import build_report, save_report


def _all_passing():
    """Return a list of two passing AuditResults."""
    return [
        AuditResult("completeness", True, {}),
        AuditResult("uniqueness", True, {}),
    ]


def _one_failing():
    """Return a list with one failing AuditResult."""
    return [
        AuditResult("completeness", True, {}),
        AuditResult("uniqueness", False, {"duplicate_emails": ["x@y.com"]}),
    ]


@pytest.mark.unit
def test_build_report_all_passing_returns_overall_pass_true():
    """overall_pass must be True when every check passed."""
    report = build_report(_all_passing(), {"customers": 0, "transactions": 0})
    assert report["overall_pass"] is True


@pytest.mark.unit
def test_build_report_one_failing_returns_overall_pass_false():
    """overall_pass must be False when any check failed."""
    report = build_report(_one_failing(), {"customers": 1, "transactions": 1})
    assert report["overall_pass"] is False


@pytest.mark.unit
def test_build_report_contains_required_keys():
    """Report dict must contain timestamp, audit_checks, and rejected_at_gate."""
    report = build_report(_all_passing(), {"customers": 0, "transactions": 0})
    assert "timestamp" in report
    assert "audit_checks" in report
    assert "rejected_at_gate" in report


@pytest.mark.unit
def test_save_report_writes_valid_json():
    """save_report must produce a JSON file that can be re-parsed."""
    report = build_report(_all_passing(), {"customers": 0, "transactions": 0})
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = tmp.name
    save_report(report, path)
    with open(path, encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded["overall_pass"] is True
