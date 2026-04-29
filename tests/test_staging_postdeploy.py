"""
Post-deployment live validation suite for the staging slot.

These tests require real network access and deployed infrastructure.
They are skipped automatically when STAGING_URL is not set, so local
dev runs (pytest -m deployment_live) exit cleanly with SKIP.

Required env vars:
  STAGING_URL                              — e.g. https://qms-sales-report-staging.azurewebsites.net
  AZURE_STORAGE_REPORTING_CONNECTION_STRING — Azure Storage connection string (blob recency test only)
  STALE_THRESHOLD_HOURS                    — optional, default 26
"""

import os
import re
import pytest
import httpx
from datetime import datetime, timezone, timedelta

STAGING_URL = os.getenv("STAGING_URL", "").rstrip("/")
STALE_THRESHOLD_HOURS = float(os.getenv("STALE_THRESHOLD_HOURS", "26"))

skip_no_staging = pytest.mark.skipif(
    not STAGING_URL,
    reason="STAGING_URL not set — skipping live staging checks",
)


@pytest.mark.deployment_live
@skip_no_staging
def test_healthz_endpoint_returns_blob_freshness():
    """
    GET /healthz must return 200 with outputs_age_hours below the stale threshold.
    Proves the app is running and blob outputs are recent.
    """
    resp = httpx.get(f"{STAGING_URL}/healthz", timeout=15)
    assert resp.status_code == 200, f"/healthz returned {resp.status_code}"
    health = resp.json()
    assert health.get("status") == "ok", f"Unexpected health status: {health.get('status')!r}"
    assert "outputs_age_hours" in health, "outputs_age_hours missing from /healthz response"
    age = health["outputs_age_hours"]
    assert age is not None, "outputs_age_hours is None — blob integration may be broken"
    assert age < STALE_THRESHOLD_HOURS, (
        f"Blob outputs are stale: {age:.1f}h > {STALE_THRESHOLD_HOURS}h threshold"
    )


@pytest.mark.deployment_live
@skip_no_staging
def test_blob_storage_has_recent_outputs():
    """
    List the reporting-outputs container and assert at least one
    qry_unified_mapped_*.csv blob was written within the stale threshold.
    Proves the refresh timer ran successfully.
    """
    conn_str = os.getenv("AZURE_STORAGE_REPORTING_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("AZURE_STORAGE_REPORTING_CONNECTION_STRING not set")

    from azure.storage.blob import BlobServiceClient

    client = BlobServiceClient.from_connection_string(conn_str)
    container = client.get_container_client("reporting-outputs")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=STALE_THRESHOLD_HOURS)

    recent_mapped = [
        b.name
        for b in container.list_blobs()
        if "qry_unified_mapped" in b.name and b.last_modified > cutoff
    ]

    assert len(recent_mapped) > 0, (
        f"No recent qry_unified_mapped_*.csv found in reporting-outputs "
        f"(checked last {STALE_THRESHOLD_HOURS:.0f}h). "
        f"Refresh timer may not have run."
    )


@pytest.mark.deployment_live
@skip_no_staging
def test_request_id_present_in_response_headers():
    """
    GET /version must include an x-request-id header matching 12-char hex.
    Proves _RequestLoggingMiddleware is active and correlation IDs are emitted.
    """
    resp = httpx.get(f"{STAGING_URL}/version", timeout=15)
    assert resp.status_code == 200, f"/version returned {resp.status_code}"
    rid = resp.headers.get("x-request-id")
    assert rid is not None, "x-request-id header missing from /version response"
    assert re.match(r"^[0-9a-f]{12}$", rid), (
        f"x-request-id '{rid}' does not match expected 12-char hex format"
    )
