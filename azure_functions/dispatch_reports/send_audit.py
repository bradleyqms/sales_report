"""Dispatch send-status audit helpers."""
from __future__ import annotations

import datetime
import json
import logging
import os
import uuid
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # pragma: no cover
    BlobServiceClient = None  # type: ignore[assignment,misc]

LOG = logging.getLogger(__name__)


def _resolve_report_date_token(report_date: datetime.datetime | None) -> str:
    if report_date is None:
        return datetime.datetime.utcnow().strftime("%Y-%m-%d")
    return report_date.strftime("%Y-%m-%d")


def _resolve_mode(mode: str | None) -> str:
    return (mode or "MTD").strip().upper() or "MTD"


def _upload_audit_blob(payload: dict, blob_name: str) -> bool:
    if BlobServiceClient is None:
        return False
    conn = os.getenv("AZURE_STORAGE_REPORTING_CONNECTION_STRING", "").strip()
    container = os.getenv("REPORTING_AUDIT_BLOB_CONTAINER", "reporting-audit").strip()
    if not conn or not container:
        return False
    try:
        service = BlobServiceClient.from_connection_string(conn)
        client = service.get_container_client(container)
        client.upload_blob(blob_name, json.dumps(payload, indent=2).encode("utf-8"), overwrite=False)
        return True
    except Exception as exc:  # pylint: disable=broad-except
        LOG.warning("Could not upload dispatch audit blob %s: %s", blob_name, exc)
        return False


def _write_local_fallback(payload: dict, outputs_dir: Path) -> str:
    fallback_dir = outputs_dir / "dispatch_audit"
    fallback_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    path = fallback_dir / f"dispatch_audit_{timestamp}_{suffix}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def record_dispatch_status(
    *,
    outputs_dir: Path,
    stream: str,
    status: str,
    recipients: list[str],
    subject: str,
    body_type: str,
    report_date: datetime.datetime | None,
    mode: str | None,
    html_files: list[Path] | None = None,
    attachments: list[Path] | None = None,
    error: str | None = None,
    details: dict | None = None,
) -> str:
    """Persist a dispatch send-status event and return its storage location."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    date_token = _resolve_report_date_token(report_date)
    resolved_mode = _resolve_mode(mode)

    payload = {
        "event_type": "dispatch_send_status",
        "created_at_utc": now_utc.isoformat(),
        "stream": stream,
        "status": status,
        "mode": resolved_mode,
        "report_date": date_token,
        "subject": subject,
        "body_type": body_type,
        "recipients": recipients,
        "attachment_files": [p.name for p in (attachments or [])],
        "html_files": [p.name for p in (html_files or [])],
        "error": error,
        "details": details or {},
    }

    blob_name = (
        f"dispatch/date={date_token}/mode={resolved_mode}/stream={stream}/status={status}/"
        f"dispatch_{timestamp}_{suffix}.json"
    )
    if _upload_audit_blob(payload, blob_name):
        return f"blob://{blob_name}"

    local_path = _write_local_fallback(payload, outputs_dir)
    return local_path
