"""Timer-triggered mapping sync between SharePoint and reporting-inputs blob."""
from __future__ import annotations

import logging

import azure.functions as func

from dispatch_reports.mapping_sync import run_mapping_sync

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def main(mytimer: func.TimerRequest = None) -> None:
    LOG.info("[DATA] mapping_inputs_sync triggered: is_past_due=%s", getattr(mytimer, "past_due", False))
    summary = run_mapping_sync()
    LOG.info(
        "[DATA] mapping_inputs_sync complete: status=%s source_of_truth=%s total=%d copied=%d in_sync=%d errors=%d",
        summary.get("status"),
        summary.get("source_of_truth"),
        summary.get("total_items", 0),
        summary.get("copied", 0),
        summary.get("in_sync", 0),
        summary.get("errors", 0),
    )
    for _result in summary.get("results", []):
        LOG.info(
            "[DATA] sync_item: name=%s action=%s sp_sha256=%.8s blob_sha256=%.8s",
            _result.get("name"),
            (_result.get("action") or "unknown"),
            (_result.get("sp_sha256") or "none"),
            (_result.get("blob_sha256") or "none"),
        )
