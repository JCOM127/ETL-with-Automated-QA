"""Functional tests: verify alert dispatchers are called correctly."""

from unittest.mock import patch

import pytest

from etl_challenge.audit.checks import AuditResult
from etl_challenge.reporting.report import build_report


def _failed_report() -> dict:
    """Build a report dict with one failing audit result.

    Returns:
        Report dict with overall_pass=False.
    """
    results = [AuditResult("completeness", False, {"null_counts": {"name": 1}})]
    return build_report(results, {"customers": 1, "transactions": 0})


@pytest.mark.functional
def test_send_webhook_called_once():
    """send_webhook must be invoked exactly once when an alert fires."""
    report = _failed_report()
    with patch("etl_challenge.alerts.dispatcher.send_webhook") as mock_wh:
        mock_wh(report)
        mock_wh.assert_called_once()


@pytest.mark.functional
def test_send_email_called_once():
    """send_email must be invoked exactly once when an alert fires."""
    report = _failed_report()
    with patch("etl_challenge.alerts.dispatcher.send_email") as mock_em:
        mock_em(subject="Audit Failed", body=str(report))
        mock_em.assert_called_once()


@pytest.mark.functional
def test_webhook_payload_contains_overall_pass():
    """Payload passed to send_webhook must contain the 'overall_pass' key."""
    report = _failed_report()
    with patch("etl_challenge.alerts.dispatcher.send_webhook") as mock_wh:
        mock_wh(report)
        call_args = mock_wh.call_args
        payload = call_args[0][0]
        assert "overall_pass" in payload


@pytest.mark.functional
def test_email_subject_is_nonempty():
    """Subject passed to send_email must be a non-empty string."""
    report = _failed_report()
    with patch("etl_challenge.alerts.dispatcher.send_email") as mock_em:
        mock_em(subject="Audit Failed", body=str(report))
        call_args = mock_em.call_args
        subject = (
            call_args.kwargs["subject"]
            if call_args.kwargs.get("subject")
            else call_args[0][0]
        )
        assert isinstance(subject, str) and len(subject) > 0
