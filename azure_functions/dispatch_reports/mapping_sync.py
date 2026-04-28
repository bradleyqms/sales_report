"""Hash-checked reconciliation between SharePoint mappings and blob mappings."""
from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlparse

import msal
import requests

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # pragma: no cover
    BlobServiceClient = None  # type: ignore[assignment,misc]

LOG = logging.getLogger(__name__)

SP_DEFAULT_BASE = "/sites/DATAANDREPORTING/Shared Documents/SAP Extracts"


@dataclass(frozen=True)
class SyncItem:
    name: str
    blob_path: str


SYNC_ITEMS = [
    SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv"),
    SyncItem(name="py25_regional_mappings.csv", blob_path="mappings/py25_regional_mappings.csv"),
]


class SharePointGraphClient:
    def __init__(self) -> None:
        site_url = os.getenv("SHAREPOINT_SITE_URL", "").strip()
        client_id = os.getenv("SHAREPOINT_CLIENT_ID", "").strip()
        client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "").strip()
        tenant = (
            os.getenv("SHAREPOINT_TENANT_ID", "").strip()
            or os.getenv("SHAREPOINT_TENANT_DOMAIN", "").strip()
            or "qmsmedicosmetics.onmicrosoft.com"
        )
        if not all([site_url, client_id, client_secret]):
            raise RuntimeError("Missing SharePoint credentials (SHAREPOINT_SITE_URL/CLIENT_ID/CLIENT_SECRET)")

        self.site_url = site_url
        self.timeout = float(os.getenv("SHAREPOINT_REQUEST_TIMEOUT_SECONDS", "30") or "30")
        self._headers = self._build_headers(tenant, client_id, client_secret)
        self.site_id = self._resolve_site_id(site_url)

    def _build_headers(self, tenant: str, client_id: str, client_secret: str) -> dict:
        authority = f"https://login.microsoftonline.com/{tenant}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret,
        )
        token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        access_token = token.get("access_token")
        if not access_token:
            raise RuntimeError(f"Failed to acquire SharePoint Graph token: {token}")
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def _resolve_site_id(self, site_url: str) -> str:
        parsed = urlparse(site_url)
        endpoint = f"https://graph.microsoft.com/v1.0/sites/{parsed.netloc}:/{parsed.path.strip('/')}"
        response = requests.get(endpoint, headers=self._headers, timeout=self.timeout)
        response.raise_for_status()
        return response.json()["id"]

    def _content_endpoint(self, server_relative_path: str) -> str:
        parsed = urlparse(self.site_url)
        site_prefix = parsed.path
        if server_relative_path.startswith(site_prefix):
            relative_path = server_relative_path[len(site_prefix):].strip("/")
        else:
            relative_path = server_relative_path.strip("/")

        if relative_path.startswith("Shared Documents/"):
            item_path = relative_path[len("Shared Documents/"):]
        else:
            item_path = relative_path

        encoded = quote(item_path)
        return f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{encoded}:/content"

    def download(self, server_relative_path: str) -> Optional[bytes]:
        endpoint = self._content_endpoint(server_relative_path)
        response = requests.get(endpoint, headers=self._headers, timeout=self.timeout)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.content

    def upload(self, server_relative_path: str, content: bytes) -> None:
        endpoint = self._content_endpoint(server_relative_path)
        headers = dict(self._headers)
        headers["Content-Type"] = "application/octet-stream"
        response = requests.put(endpoint, headers=headers, data=content, timeout=self.timeout)
        response.raise_for_status()


class BlobInputsClient:
    def __init__(self) -> None:
        if BlobServiceClient is None:
            raise RuntimeError("azure-storage-blob is not available")
        conn = os.getenv("AZURE_STORAGE_REPORTING_CONNECTION_STRING", "").strip()
        container = os.getenv("REPORTING_INPUTS_BLOB_CONTAINER", "reporting-inputs").strip()
        if not conn or not container:
            raise RuntimeError("Missing AZURE_STORAGE_REPORTING_CONNECTION_STRING or REPORTING_INPUTS_BLOB_CONTAINER")
        service = BlobServiceClient.from_connection_string(conn)
        self.container_client = service.get_container_client(container)

    def download(self, blob_path: str) -> Optional[bytes]:
        blob_client = self.container_client.get_blob_client(blob_path)
        try:
            return blob_client.download_blob().readall()
        except Exception:
            return None

    def upload(self, blob_path: str, content: bytes) -> None:
        blob_client = self.container_client.get_blob_client(blob_path)
        blob_client.upload_blob(content, overwrite=True)


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _resolve_source_of_truth() -> str:
    value = os.getenv("MAPPING_SYNC_SOURCE_OF_TRUTH", "sharepoint").strip().lower()
    return "blob" if value == "blob" else "sharepoint"


def _resolve_sp_path(filename: str) -> str:
    base = os.getenv("MAPPING_SYNC_SHAREPOINT_BASE_PATH", SP_DEFAULT_BASE).strip().rstrip("/")
    return f"{base}/{filename}"


def _write_sync_audit(payload: dict) -> str:
    conn = os.getenv("AZURE_STORAGE_REPORTING_CONNECTION_STRING", "").strip()
    container = os.getenv("REPORTING_AUDIT_BLOB_CONTAINER", "reporting-audit").strip()
    date_token = payload.get("date", datetime.datetime.utcnow().strftime("%Y-%m-%d"))
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    blob_name = (
        f"sync/date={date_token}/"
        f"mapping_sync_{ts}_{uuid.uuid4().hex[:8]}.json"
    )

    data = json.dumps(payload, indent=2).encode("utf-8")
    if BlobServiceClient is not None and conn and container:
        service = BlobServiceClient.from_connection_string(conn)
        client = service.get_container_client(container)
        client.upload_blob(blob_name, data, overwrite=False)
        return f"blob://{container}/{blob_name}"

    temp_dir = Path(tempfile.gettempdir()) / "mapping_sync_audit"
    temp_dir.mkdir(parents=True, exist_ok=True)
    out_path = temp_dir / Path(blob_name).name
    out_path.write_bytes(data)
    return str(out_path)


def _reconcile_item(
    item: SyncItem,
    sp_client: SharePointGraphClient,
    blob_client: BlobInputsClient,
    source_of_truth: str,
    dry_run: bool,
) -> dict:
    sp_path = _resolve_sp_path(item.name)
    sp_content = sp_client.download(sp_path)
    blob_content = blob_client.download(item.blob_path)

    result = {
        "name": item.name,
        "sp_path": sp_path,
        "blob_path": item.blob_path,
        "sp_exists": sp_content is not None,
        "blob_exists": blob_content is not None,
        "action": "none",
        "source_of_truth": source_of_truth,
        "status": "ok",
    }

    sp_hash = _sha256_bytes(sp_content) if sp_content is not None else None
    blob_hash = _sha256_bytes(blob_content) if blob_content is not None else None
    result["sp_sha256"] = sp_hash
    result["blob_sha256"] = blob_hash

    if sp_content is None and blob_content is None:
        result["action"] = "missing_both"
        result["status"] = "warning"
        return result

    if sp_content is not None and blob_content is not None and sp_hash == blob_hash:
        result["action"] = "in_sync"
        return result

    if sp_content is None and blob_content is not None:
        if not dry_run:
            sp_client.upload(sp_path, blob_content)
        result["action"] = "copied_blob_to_sharepoint"
        return result

    if blob_content is None and sp_content is not None:
        if not dry_run:
            blob_client.upload(item.blob_path, sp_content)
        result["action"] = "copied_sharepoint_to_blob"
        return result

    # Both exist and differ -> enforce source-of-truth
    if source_of_truth == "sharepoint":
        if not dry_run and sp_content is not None:
            blob_client.upload(item.blob_path, sp_content)
        result["action"] = "reconciled_sharepoint_to_blob"
    else:
        if not dry_run and blob_content is not None:
            sp_client.upload(sp_path, blob_content)
        result["action"] = "reconciled_blob_to_sharepoint"

    return result


def run_mapping_sync() -> dict:
    source_of_truth = _resolve_source_of_truth()
    dry_run = os.getenv("MAPPING_SYNC_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "on"}

    summary = {
        "status": "ok",
        "date": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
        "ran_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "source_of_truth": source_of_truth,
        "dry_run": dry_run,
        "results": [],
    }

    try:
        sp_client = SharePointGraphClient()
        blob_client = BlobInputsClient()

        copied = 0
        in_sync = 0
        errors = 0

        for item in SYNC_ITEMS:
            try:
                result = _reconcile_item(item, sp_client, blob_client, source_of_truth, dry_run)
            except Exception as exc:  # pylint: disable=broad-except
                result = {
                    "name": item.name,
                    "status": "error",
                    "action": "error",
                    "error": str(exc),
                }
                errors += 1

            action = result.get("action")
            if action in {
                "copied_blob_to_sharepoint",
                "copied_sharepoint_to_blob",
                "reconciled_sharepoint_to_blob",
                "reconciled_blob_to_sharepoint",
            }:
                copied += 1
            if action == "in_sync":
                in_sync += 1
            if result.get("status") == "error":
                errors += 1
            summary["results"].append(result)

        summary["total_items"] = len(SYNC_ITEMS)
        summary["copied"] = copied
        summary["in_sync"] = in_sync
        summary["errors"] = errors
        if errors > 0:
            summary["status"] = "amber"

    except Exception as exc:  # pylint: disable=broad-except
        summary["status"] = "failed"
        summary["errors"] = summary.get("errors", 0) + 1
        summary["fatal_error"] = str(exc)

    try:
        location = _write_sync_audit(summary)
        summary["audit_location"] = location
    except Exception as audit_exc:  # pylint: disable=broad-except
        LOG.warning("Failed to write mapping sync audit: %s", audit_exc)

    LOG.info(
        "Mapping sync status=%s source_of_truth=%s total=%s copied=%s in_sync=%s errors=%s",
        summary.get("status"),
        summary.get("source_of_truth"),
        summary.get("total_items", 0),
        summary.get("copied", 0),
        summary.get("in_sync", 0),
        summary.get("errors", 0),
    )
    return summary
