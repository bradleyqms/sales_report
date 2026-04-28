"""Timer-triggered mapping sync between SharePoint and reporting-inputs blob."""
from __future__ import annotations

import logging

import azure.functions as func

from dispatch_reports.mapping_sync import run_mapping_sync

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def main(mytimer: func.TimerRequest = None) -> None:
    summary = run_mapping_sync()
    LOG.info(
        "Mapping sync completed: status=%s source_of_truth=%s total=%d copied=%d in_sync=%d errors=%d",
        summary.get("status"),
        summary.get("source_of_truth"),
        summary.get("total_items", 0),
        summary.get("copied", 0),
        summary.get("in_sync", 0),
        summary.get("errors", 0),
    )
