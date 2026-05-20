"""OIDC bearer-token validation for the /classify/* endpoints.

Cloud Scheduler signs each request with a Google-issued OIDC JWT whose `aud`
is the configured audience and whose `email` is the scheduler service account.
We verify both before letting the classify cycle run.

Local dev bypasses validation when CLASSIFY_OIDC_AUDIENCE is unset.
"""

from __future__ import annotations

import logging
import os
from functools import wraps
from typing import Callable

from flask import abort, request

logger = logging.getLogger(__name__)


def _verify(token: str, audience: str) -> dict:
    # Imported lazily so test environments without google-auth still load the module.
    from google.auth.transport import requests as ga_requests
    from google.oauth2 import id_token

    return id_token.verify_oauth2_token(token, ga_requests.Request(), audience=audience)


def require_scheduler_oidc(view: Callable) -> Callable:
    """Decorator: enforce a valid Cloud Scheduler OIDC token on the wrapped view.

    No-op when `CLASSIFY_OIDC_AUDIENCE` is unset (local dev / tests).
    """

    @wraps(view)
    def wrapper(*args, **kwargs):
        audience = os.environ.get("CLASSIFY_OIDC_AUDIENCE")
        if not audience:
            return view(*args, **kwargs)

        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            logger.warning("classify: missing or malformed Authorization header")
            abort(401)
        token = header[len("Bearer ") :].strip()

        try:
            claims = _verify(token, audience)
        except Exception as e:
            logger.warning("classify: OIDC verify failed: %s", e)
            abort(401)

        expected_email = os.environ.get("CLASSIFY_OIDC_SA_EMAIL")
        if expected_email and claims.get("email") != expected_email:
            logger.warning(
                "classify: token email %s does not match expected %s",
                claims.get("email"),
                expected_email,
            )
            abort(403)

        return view(*args, **kwargs)

    return wrapper
