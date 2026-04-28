"""EOD dispatch digest builder."""
from __future__ import annotations

import datetime
import json
import logging
import os
from collections import defaultdict

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # pragma: no cover
    BlobServiceClient = None  # type: ignore[assignment,misc]

LOG = logging.getLogger(__name__)


def resolve_digest_date() -> str:
    override = os.getenv("EOD_DISPATCH_DIGEST_DATE", "").strip()
    if override:
        return override
    d = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).date()
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def _list_audit_records(date_token: str) -> list[dict]:
    if BlobServiceClient is None:
        return []

    conn = os.getenv("AZURE_STORAGE_REPORTING_CONNECTION_STRING", "").strip()
    container = os.getenv("REPORTING_AUDIT_BLOB_CONTAINER", "reporting-audit").strip()
    if not conn or not container:
        return []

    prefix = f"dispatch/date={date_token}/"
    service = BlobServiceClient.from_connection_string(conn)
    client = service.get_container_client(container)

    records: list[dict] = []
    for blob in client.list_blobs(name_starts_with=prefix):
        raw = client.download_blob(blob.name).readall().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            LOG.warning("Skipping non-JSON dispatch audit blob: %s", blob.name)
            continue
        payload["_blob_name"] = blob.name
        records.append(payload)
    return records


def aggregate_dispatch_health(records: list[dict]) -> dict:
    totals = defaultdict(int)
    by_stream = defaultdict(lambda: defaultdict(int))

    for record in records:
        stream = (record.get("stream") or "unknown").strip() or "unknown"
        status = (record.get("status") or "unknown").strip().lower() or "unknown"
        totals[status] += 1
        by_stream[stream][status] += 1

    health = "green"
    if totals.get("failed", 0) > 0 or totals.get("skipped", 0) > 0:
        health = "amber"

    return {
        "health": health,
        "total_events": int(sum(totals.values())),
        "totals": {k: int(v) for k, v in totals.items()},
        "by_stream": {stream: {k: int(v) for k, v in stats.items()} for stream, stats in by_stream.items()},
    }


def build_digest_html(date_token: str, aggregate: dict, records: list[dict]) -> str:
    rows = []
    for stream, stats in sorted(aggregate.get("by_stream", {}).items()):
        sent = stats.get("sent", 0)
        failed = stats.get("failed", 0)
        skipped = stats.get("skipped", 0)
        rows.append(
            f"<tr><td>{stream}</td><td>{sent}</td><td>{failed}</td><td>{skipped}</td></tr>"
        )

    amber_notes = []
    if aggregate.get("totals", {}).get("failed", 0) > 0:
        amber_notes.append("One or more dispatch sends failed.")
    if aggregate.get("totals", {}).get("skipped", 0) > 0:
        amber_notes.append("One or more dispatch streams were skipped.")

    failed_items = []
    for record in records:
        if (record.get("status") or "").lower() != "failed":
            continue
        stream = record.get("stream") or "unknown"
        error = record.get("error") or "(no error text)"
        failed_items.append(f"<li><b>{stream}</b>: {error}</li>")

    amber_section = ""
    if amber_notes or failed_items:
        amber_section = """
            <h3>Amber Notes</h3>
            <ul>{notes}</ul>
            <h3>Failed Events</h3>
            <ul>{failed}</ul>
        """.format(
            notes="".join(f"<li>{note}</li>" for note in amber_notes) or "<li>None</li>",
            failed="".join(failed_items) or "<li>None</li>",
        )

    return f"""
<html>
  <body style=\"font-family: Segoe UI, Arial, sans-serif; color: #111827;\">
    <h2>Dispatch EOD Health Digest ({date_token})</h2>
    <p><b>Overall Status:</b> {aggregate.get('health', 'green').upper()}</p>
    <p><b>Total Events:</b> {aggregate.get('total_events', 0)}</p>

    <h3>Per Stream</h3>
    <table border=\"1\" cellpadding=\"6\" cellspacing=\"0\" style=\"border-collapse: collapse;\">
      <thead>
        <tr><th>Stream</th><th>Sent</th><th>Failed</th><th>Skipped</th></tr>
      </thead>
      <tbody>
        {''.join(rows) if rows else '<tr><td colspan="4">No events recorded</td></tr>'}
      </tbody>
    </table>
    {amber_section}
  </body>
</html>
""".strip()


def collect_digest_payload() -> tuple[str, dict, list[dict]]:
    date_token = resolve_digest_date()
    records = _list_audit_records(date_token)
    aggregate = aggregate_dispatch_health(records)
    return date_token, aggregate, records
