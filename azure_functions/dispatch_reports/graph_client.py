"""Microsoft Graph API client — token acquisition and email dispatch."""
from __future__ import annotations

import base64
import logging
import mimetypes
import os
from pathlib import Path

import msal
import requests

LOG = logging.getLogger(__name__)

GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
SCOPES = ["https://graph.microsoft.com/.default"]


def acquire_graph_token() -> str | None:
    """Acquire an OAuth2 bearer token using client credentials flow."""
    tenant = os.getenv("GRAPH_TENANT_ID")
    client_id = os.getenv("GRAPH_CLIENT_ID")
    client_secret = os.getenv("GRAPH_CLIENT_SECRET")
    if not all((tenant, client_id, client_secret)):
        LOG.warning("Missing GRAPH_* credentials — cannot acquire token")
        return None
    authority = f"https://login.microsoftonline.com/{tenant}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(SCOPES)
    if not result or "access_token" not in result:
        LOG.error("Could not acquire Graph token: %s", result)
        return None
    return result["access_token"]


def prepare_graph_attachments(paths: list[Path]) -> list[dict]:
    """Base64-encode files into Graph fileAttachment objects."""
    attachments: list[dict] = []
    for path in paths:
        mime_type, _ = mimetypes.guess_type(path.name)
        content = base64.b64encode(path.read_bytes()).decode("utf-8")
        attachments.append(
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": path.name,
                "contentType": mime_type or "application/octet-stream",
                "contentBytes": content,
            }
        )
    return attachments


def send_via_graph(
    recipients: list[str],
    attachments: list[Path],
    body: str,
    subject: str,
    body_content_type: str = "Text",
) -> None:
    """Send an email via Microsoft Graph sendMail.

    Raises:
        requests.HTTPError: if the Graph API returns a non-2xx status.
        ValueError: if REPORT_DISPATCH_GRAPH_SENDER is not configured.
    """
    sender = os.getenv("REPORT_DISPATCH_GRAPH_SENDER")
    if not sender:
        raise ValueError("REPORT_DISPATCH_GRAPH_SENDER is required to send via Graph")

    token = acquire_graph_token()
    if not token:
        raise RuntimeError("Failed to acquire Graph access token")

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": body_content_type, "content": body},
            "toRecipients": [
                {"emailAddress": {"address": r}} for r in recipients
            ],
            "attachments": prepare_graph_attachments(attachments),
        },
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        GRAPH_ENDPOINT.format(sender=sender),
        json=payload,
        headers=headers,
        timeout=60,
    )
    if response.status_code >= 300:
        LOG.error(
            "Graph sendMail failed (%s): %s", response.status_code, response.text
        )
        response.raise_for_status()
    LOG.info(
        "Sent %d attachment(s) via Graph to %s", len(attachments), recipients
    )
