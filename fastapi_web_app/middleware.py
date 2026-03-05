"""
middleware.py — AuthMiddleware

Reads the Azure Easy Auth header (X-MS-CLIENT-PRINCIPAL-NAME) once per request,
builds an immutable UserContext, and attaches it to request.state.user.

Local dev fallback: if the header is absent, DEV_USER_EMAIL env var is used.
This runs before any route handler — no Depends() threading required.
"""
import os
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from auth import create_user_context, ANONYMOUS

logger = logging.getLogger(__name__)

# Azure Easy Auth header name
_EASY_AUTH_HEADER = "x-ms-client-principal-name"


class AuthMiddleware(BaseHTTPMiddleware):
    """
    FastAPI/Starlette middleware that resolves the authenticated user once per
    request and stores the result in request.state.user (a frozen UserContext).
    """

    async def dispatch(self, request: Request, call_next):
        email = (
            request.headers.get(_EASY_AUTH_HEADER)          # Azure Easy Auth (production)
            or os.environ.get("DEV_USER_EMAIL", "").strip()  # local dev fallback
        )

        if email:
            request.state.user = create_user_context(email)
        else:
            # Should not reach here post-Easy Auth, but be defensive
            logger.warning("[auth] No user identity found for request: %s", request.url.path)
            request.state.user = ANONYMOUS

        response = await call_next(request)
        return response
