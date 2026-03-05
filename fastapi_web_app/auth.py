"""
auth.py — UserContext dataclass + startup permission sets.

Part A: UserContext frozen dataclass — immutable for the lifetime of each request.
Part B: Module-level frozensets parsed from env vars at startup (O(1) lookups).
        create_user_context() is the single factory used by middleware.
"""
from __future__ import annotations

import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ============================================================
# Part B — Startup: parse env-var permission sets ONCE
# ============================================================

def get_env_set(var_name: str) -> frozenset[str]:
    """Parse a comma-separated env var into a frozenset of lowercase, stripped emails."""
    raw = os.environ.get(var_name, "")
    return frozenset(e.strip().lower() for e in raw.split(",") if e.strip())


# Parsed at module import time — never re-computed per request
ADMINS:     frozenset[str] = get_env_set("HUB_ADMIN_EMAILS")
MANAGEMENT: frozenset[str] = get_env_set("HUB_MANAGEMENT_EMAILS")
CORE:       frozenset[str] = get_env_set("HUB_CORE_EMAILS")
USA:        frozenset[str] = get_env_set("HUB_USA_EMAILS")

logger.info("[auth] Permission sets loaded — ADMINS=%s | MANAGEMENT=%s | CORE=%s | USA=%s",
            set(ADMINS), set(MANAGEMENT), set(CORE), set(USA))


# ============================================================
# Part A — UserContext frozen dataclass
# ============================================================

@dataclass(frozen=True)
class UserContext:
    """
    Immutable user identity attached to request.state.user for the lifetime of a request.
    All properties are derived from the email at construction time.
    """
    email: str
    is_admin:      bool
    is_management: bool
    is_core:       bool
    is_usa:        bool

    # ----- derived display properties -----------------------------------------

    @property
    def display_name(self) -> str:
        """
        'firstname.lastname@domain.com' → 'Firstname L.'
        Handles '.', '_', '-' separators.  Falls back to 'User' for unconventional addresses.
        """
        try:
            local = self.email.split("@")[0]
            # Replace underscores / hyphens so we always split on dots
            local = local.replace("_", ".").replace("-", ".")
            parts = [p for p in local.split(".") if p]
            if len(parts) >= 2:
                first = parts[0].capitalize()
                last_initial = parts[-1][0].upper()
                return f"{first} {last_initial}."
            elif len(parts) == 1:
                return parts[0].capitalize()
            return "User"
        except Exception:
            return "User"

    @property
    def group_label(self) -> str:
        """Priority-ordered group label for display."""
        if self.is_admin:
            return "Admin"
        if self.is_management:
            return "Management"
        if self.is_core:
            return "Core Markets"
        if self.is_usa:
            return "USA Spa"
        return "Viewer"

    @property
    def avatar_char(self) -> str:
        """Single uppercase character for the avatar circle."""
        try:
            return self.email[0].upper()
        except Exception:
            return "?"


# ============================================================
# Factory
# ============================================================

def create_user_context(email: str) -> UserContext:
    """
    Build a UserContext from a raw email string.
    Normalises email to lowercase once, then does four O(1) frozenset lookups.
    """
    e = email.lower().strip()
    return UserContext(
        email=e,
        is_admin=      e in ADMINS,
        is_management= e in MANAGEMENT,
        is_core=       e in CORE,
        is_usa=        e in USA,
    )


# Sentinel for unauthenticated state (used by middleware when no email is found)
ANONYMOUS = UserContext(
    email="anonymous@unknown",
    is_admin=False,
    is_management=False,
    is_core=False,
    is_usa=False,
)
