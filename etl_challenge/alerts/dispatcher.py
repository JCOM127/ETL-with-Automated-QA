"""Alert dispatchers for audit failures.

Both functions have production-ready signatures but stub bodies that
raise NotImplementedError. All calls in tests are intercepted via
unittest.mock.patch before the error is ever reached.
"""


def send_webhook(
    payload: dict,
    url: str = "https://mock.webhook.example/alert",
) -> None:
    """POST an audit payload to a webhook endpoint.

    In production this would call requests.post(url, json=payload) and
    raise on non-2xx status codes. The function is a stub here so the
    pipeline runs without any network dependency; all test calls are
    intercepted via unittest.mock.patch.

    Args:
        payload: The audit report dict to send as JSON.
        url: Destination webhook URL. Defaults to a placeholder.

    Raises:
        NotImplementedError: Always, until a real HTTP client is wired in.
    """
    raise NotImplementedError(
        "send_webhook is a stub. In production, POST payload to url via requests.post()."
    )


def send_email(
    subject: str,
    body: str,
    to: str = "data-team@example.com",
) -> None:
    """Send an audit alert email via SMTP or a provider SDK.

    In production this would connect to an SMTP server or call a
    provider SDK such as SendGrid. The function is a stub here so the
    pipeline runs without any email dependency; all test calls are
    intercepted via unittest.mock.patch.

    Args:
        subject: Email subject line; should be non-empty.
        body: Plain-text or HTML body of the alert email.
        to: Recipient address. Defaults to the data team alias.

    Raises:
        NotImplementedError: Always, until a real email client is wired in.
    """
    raise NotImplementedError(
        "send_email is a stub. In production, send via SMTP or a provider SDK."
    )
