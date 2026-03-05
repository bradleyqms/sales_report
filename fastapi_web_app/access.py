"""
access.py — Thin route guards that read request.state.user.

Middleware has already populated request.state.user before any route runs, so
these helpers never need to call Depends() or re-parse identity.

Usage in route handlers:
    assert_admin(request)                          # admin-only
    check_access(request, "core", "management", "admin")  # one of these tiers
"""
from fastapi import HTTPException, Request


def check_access(request: Request, *tiers: str) -> None:
    """
    Raise HTTP 403 if the authenticated user does not belong to at least one
    of the given tiers.

    Tier strings: "admin", "management", "core", "usa"
    """
    user = getattr(request.state, "user", None)
    if user is None:
        raise HTTPException(status_code=403, detail="Not authenticated")

    tier_map = {
        "admin":      user.is_admin,
        "management": user.is_management,
        "core":       user.is_core,
        "usa":        user.is_usa,
    }

    if any(tier_map.get(t, False) for t in tiers):
        return  # access granted

    raise HTTPException(status_code=403, detail="Access denied")


def assert_admin(request: Request) -> None:
    """Shorthand guard — raises 403 unless the user is an admin."""
    check_access(request, "admin")
