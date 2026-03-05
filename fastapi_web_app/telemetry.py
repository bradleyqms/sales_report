"""
telemetry.py — Best-effort telemetry logging.

All three public functions are wrapped in try/except so that any DB failure
(e.g. Azure SQL unavailable) is console-logged only — it NEVER propagates to
the user response.

Intended to be called via FastAPI BackgroundTasks:
    background_tasks.add_task(log_page_view, db, user.email, "/coremarkets")
"""
import logging
import sys
from pathlib import Path

# Allow importing src models when called from fastapi_web_app context
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import TelemetryLog  # noqa: E402  (added in Step 10)

logger = logging.getLogger(__name__)


def log_page_view(db, user_email: str, page_id: str, load_time_ms: int | None = None) -> None:
    """Record a page view event.  Silently swallows all exceptions."""
    try:
        entry = TelemetryLog(
            user_email=user_email.lower().strip(),
            event_type="page_view",
            page_id=page_id,
            load_time_ms=load_time_ms,
        )
        db.add(entry)
        db.commit()
    except Exception as exc:
        logger.warning("[telemetry] page_view log failed: %s", exc)


def log_export(db, user_email: str, file_format: str, report_type: str) -> None:
    """Record an export/download event.  Silently swallows all exceptions."""
    try:
        entry = TelemetryLog(
            user_email=user_email.lower().strip(),
            event_type="export",
            page_id="/download",
            file_format=file_format[:10] if file_format else None,
            report_type=report_type,
        )
        db.add(entry)
        db.commit()
    except Exception as exc:
        logger.warning("[telemetry] export log failed: %s", exc)


def log_admin_click(db, user_email: str, action: str, entity_id: int | None = None) -> None:
    """Record an admin action event.  Silently swallows all exceptions."""
    try:
        entry = TelemetryLog(
            user_email=user_email.lower().strip(),
            event_type="admin_click",
            page_id="/admin",
            action=action,
            entity_id=entity_id,
        )
        db.add(entry)
        db.commit()
    except Exception as exc:
        logger.warning("[telemetry] admin_click log failed: %s", exc)
