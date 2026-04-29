"""Tests for mapping sync reconciliation logic."""
from __future__ import annotations

import sys
from pathlib import Path


_HERE = Path(__file__).parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dispatch_reports import mapping_sync as _mod


class _StubSp:
    def __init__(self, payload: bytes | None = None):
        self.payload = payload
        self.uploaded = None

    def download(self, _path: str):
        return self.payload

    def upload(self, _path: str, content: bytes):
        self.uploaded = content


class _StubBlob:
    def __init__(self, payload: bytes | None = None):
        self.payload = payload
        self.uploaded = None

    def download(self, _path: str):
        return self.payload

    def upload(self, _path: str, content: bytes):
        self.uploaded = content


def test_reconcile_in_sync():
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(b"same")
    blob = _StubBlob(b"same")

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=False)
    assert result["action"] == "in_sync"


def test_reconcile_source_of_truth_sharepoint():
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(b"new_sp")
    blob = _StubBlob(b"old_blob")

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=False)
    assert result["action"] == "reconciled_sharepoint_to_blob"
    assert blob.uploaded == b"new_sp"


def test_reconcile_source_of_truth_blob():
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(b"old_sp")
    blob = _StubBlob(b"new_blob")

    result = _mod._reconcile_item(item, sp, blob, "blob", dry_run=False)
    assert result["action"] == "reconciled_blob_to_sharepoint"
    assert sp.uploaded == b"new_blob"


def test_reconcile_dry_run_does_not_write():
    """dry_run=True: action is still reported but no upload occurs."""
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(b"sp_version")
    blob = _StubBlob(b"blob_version")

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=True)
    assert result["action"] == "reconciled_sharepoint_to_blob"
    assert blob.uploaded is None  # dry_run: no write should happen


def test_reconcile_missing_both_returns_warning():
    """Both sides missing → action='missing_both', status='warning'."""
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(None)
    blob = _StubBlob(None)

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=False)
    assert result["action"] == "missing_both"
    assert result["status"] == "warning"


def test_reconcile_missing_sp_copies_blob_to_sp():
    """SP missing, blob present → copy blob → SP regardless of source_of_truth."""
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(None)
    blob = _StubBlob(b"blob_only_content")

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=False)
    assert result["action"] == "copied_blob_to_sharepoint"
    assert sp.uploaded == b"blob_only_content"


def test_reconcile_missing_blob_copies_sp_to_blob():
    """Blob missing, SP present → copy SP → blob regardless of source_of_truth."""
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(b"sp_only_content")
    blob = _StubBlob(None)

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=False)
    assert result["action"] == "copied_sharepoint_to_blob"
    assert blob.uploaded == b"sp_only_content"


def test_reconcile_dry_run_missing_sp_does_not_upload():
    """dry_run=True with missing SP: reports action but does not upload."""
    item = _mod.SyncItem(name="entity_mappings.csv", blob_path="mappings/entity_mappings.csv")
    sp = _StubSp(None)
    blob = _StubBlob(b"blob_content")

    result = _mod._reconcile_item(item, sp, blob, "sharepoint", dry_run=True)
    assert result["action"] == "copied_blob_to_sharepoint"
    assert sp.uploaded is None  # dry_run: no write
