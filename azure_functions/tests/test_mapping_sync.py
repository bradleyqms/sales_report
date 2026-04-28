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
