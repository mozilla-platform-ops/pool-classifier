"""OIDC and IAP validation for protected endpoints.

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


ADMIN_EMAILS = frozenset({"aerickson@mozilla.com"})
IAP_CERTS_URL = "https://www.gstatic.com/iap/verify/public_key"


def _verify(token: str, audience: str) -> dict:
    # Imported lazily so test environments without google-auth still load the module.
    from google.auth.transport import requests as ga_requests
    from google.oauth2 import id_token

    return id_token.verify_oauth2_token(token, ga_requests.Request(), audience=audience)


def _verify_iap(token: str, audience: str) -> dict:
    """Validate a signed IAP assertion and return its claims."""
    # Imported lazily so test environments without google-auth still load the module.
    from google.auth.transport import requests as ga_requests
    from google.oauth2 import id_token

    claims = id_token.verify_token(
        token,
        ga_requests.Request(),
        audience=audience,
        certs_url=IAP_CERTS_URL,
    )
    if claims.get("iss") != "https://cloud.google.com/iap":
        raise ValueError("unexpected IAP JWT issuer")
    return claims


def require_admin_iap(view: Callable) -> Callable:
    """Require a signed IAP assertion for one of the hardcoded administrators.

    IAP already protects browser routes at the load balancer.  This is a
    second, application-level guard: unsigned identity headers must never be
    used to grant administrative access.
    """

    @wraps(view)
    def wrapper(*args, **kwargs):
        audience = os.environ.get("IAP_JWT_AUDIENCE")
        if not audience:
            logger.error("admin: IAP_JWT_AUDIENCE is not configured")
            abort(403)

        assertion = request.headers.get("X-Goog-IAP-JWT-Assertion", "")
        if not assertion:
            logger.warning("admin: missing IAP signed assertion")
            abort(401)
        try:
            claims = _verify_iap(assertion, audience)
        except Exception as exc:  # noqa: BLE001 - verification failures are unauthorized
            logger.warning("admin: IAP assertion verification failed: %s", exc)
            abort(401)

        if claims.get("email") not in ADMIN_EMAILS:
            logger.warning("admin: IAP user %s is not authorized", claims.get("email"))
            abort(403)
        return view(*args, **kwargs)

    return wrapper


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
