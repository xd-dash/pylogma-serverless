"""Flask view functions for the Gen 1 build, mirroring handlers.go's
`runHandler` (there is no `/events` on this branch -- see runtime.py's
module docstring for why).

Registered onto the shared Flask app by router.py, which is the sole
owner of route registration/ROUTES for this package.
"""

from __future__ import annotations

import hmac
import json
import os

from flask import Request, jsonify, request

from .runtime import RuntimeHolder

# One holder per warm container process, exactly like the asyncio
# branch's app.state.runtime_holder / Go's router.go runtimeHolder --
# constructed once at import time (cold start) and reused across every
# subsequent invocation the warm instance serves.
_holder = RuntimeHolder()


def _check_api_key(req: Request):
    """Mirrors authenticateAPIKey in router.go: X-API-Key header must
    match the API_KEY env var, compared in constant time. Returns a
    Flask response tuple on failure, or None if the request may proceed.
    """
    api_key = os.environ.get("API_KEY")
    if not api_key:
        return "server misconfigured: API_KEY not set", 500
    supplied = req.headers.get("X-API-Key", "")
    if not hmac.compare_digest(supplied, api_key):
        return "unauthorized", 401
    return None


def run_view():
    """POST /run -- claims a runtime and blocks until it stops (via a
    control:shutdown publish or the platform's own function timeout;
    see runtime.py's module docstring for why client-disconnect
    cancellation isn't available on this branch). Returns
    {"status": "stopped"} once the runtime ends, or 409 if a runtime is
    already claimed.
    """
    auth_error = _check_api_key(request)
    if auth_error is not None:
        return auth_error

    rt = _holder.claim()
    if rt is None:
        return "runtime already claimed", 409

    rt.start()

    return jsonify({"status": "stopped"})
