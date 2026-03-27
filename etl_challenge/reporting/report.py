"""Build and persist the JSON audit report.

The report captures both Pydantic gate rejections and post-load audit
outcomes in one document, making the pipeline self-auditing.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from etl_challenge.audit.checks import AuditResult


def build_report(
    results: list[AuditResult],
    rejected_counts: dict,
) -> dict:
    """Assemble a structured audit report dictionary.

    Args:
        results: List of AuditResult objects from all QA checks.
        rejected_counts: Dict with keys 'customers' and 'transactions'
            holding the integer counts of gate rejections.

    Returns:
        A dict containing 'timestamp', 'overall_pass', 'audit_checks',
        and 'rejected_at_gate'.
    """
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "overall_pass": all(r.passed for r in results),
        "audit_checks": [
            {"check_name": r.check_name, "passed": r.passed, "details": r.details}
            for r in results
        ],
        "rejected_at_gate": rejected_counts,
    }


def save_report(report: dict, path: str = "audit_report.json") -> None:
    """Write the audit report to a JSON file.

    Args:
        report: The report dict produced by build_report.
        path: Filesystem path for the output file.  Defaults to
            'audit_report.json' in the current working directory.
    """
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    logger.info("Audit report saved to %s (overall_pass=%s)", path, report["overall_pass"])
