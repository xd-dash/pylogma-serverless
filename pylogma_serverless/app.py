"""ASGI app exposing the Runtime over HTTP, mirroring router.go/handlers.go.

Routes:
    POST /run     -- claims a runtime and blocks until it stops. No SSE;
                      returns {"status": "stopped"} once the runtime ends.
    GET  /events  -- claims a runtime, starts it in the background, and
                      streams its events as SSE until it stops or the
                      client disconnects.

Deliberately no `/` health-check route: with maxInstanceRequestConcurrency=1
(Cloud Run / Cloud Functions Gen 2), a health check would consume the
container's only request slot, exactly as documented in router.go.

Auth: every route requires an `X-API-Key` header matching the API_KEY
environment variable, compared in constant time (mirrors
authenticateAPIKey in router.go, which uses crypto/subtle).
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import os
from typing import AsyncIterator

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware

from .runtime import SSE_KEEPALIVE, RuntimeHolder

logger = logging.getLogger("pylogma_serverless.app")

DISCONNECT_POLL_INTERVAL = 1.0  # seconds


class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        api_key = os.environ.get("API_KEY")
        if not api_key:
            return Response("server misconfigured: API_KEY not set", status_code=500)
        supplied = request.headers.get("X-API-Key", "")
        if not hmac.compare_digest(supplied, api_key):
            return Response("unauthorized", status_code=401)
        return await call_next(request)


async def _watch_disconnect(request: Request, stop_event: asyncio.Event) -> None:
    """Polls request.is_disconnected(), mirroring the "case <-r.Context().Done()"
    branch from Go's eventsHandler. Starlette has no push-based disconnect
    event, so this is the standard poll-based equivalent (as used by
    sse-starlette and similar libraries).
    """
    try:
        while not stop_event.is_set():
            if await request.is_disconnected():
                stop_event.set()
                return
            await asyncio.sleep(DISCONNECT_POLL_INTERVAL)
    except asyncio.CancelledError:
        pass


async def run_endpoint(request: Request) -> Response:
    holder: RuntimeHolder = request.app.state.runtime_holder
    rt = holder.claim()
    if rt is None:
        return Response("runtime already claimed", status_code=409)

    disconnect_event = asyncio.Event()
    watcher = asyncio.create_task(_watch_disconnect(request, disconnect_event))
    try:
        await rt.start(external_disconnect=disconnect_event)
    finally:
        disconnect_event.set()
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass

    return JSONResponse({"status": "stopped"})


async def events_endpoint(request: Request) -> Response:
    holder: RuntimeHolder = request.app.state.runtime_holder
    rt = holder.claim()
    if rt is None:
        return Response("runtime already claimed", status_code=409)

    disconnect_event = asyncio.Event()
    watcher = asyncio.create_task(_watch_disconnect(request, disconnect_event))
    run_task = asyncio.create_task(rt.start(external_disconnect=disconnect_event))

    async def event_stream() -> AsyncIterator[bytes]:
        try:
            yield b": connected\n\n"

            while True:
                events_get = asyncio.ensure_future(rt.events.get())
                done_wait = asyncio.ensure_future(rt.wait_done())
                try:
                    done, pending = await asyncio.wait(
                        {events_get, done_wait},
                        timeout=SSE_KEEPALIVE,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except asyncio.CancelledError:
                    events_get.cancel()
                    done_wait.cancel()
                    raise

                if not done:
                    # Timed out -- nothing arrived within the keepalive
                    # window; mirror Go's `case <-keepAlive.C`.
                    events_get.cancel()
                    done_wait.cancel()
                    yield b": keepalive\n\n"
                    continue

                if done_wait in done:
                    events_get.cancel()
                    return

                # events_get in done
                done_wait.cancel()
                req = events_get.result()
                payload = json.dumps(req.to_json_dict()).encode("utf-8")
                yield b"event: message\ndata: " + payload + b"\n\n"
        finally:
            # Mirrors `case <-r.Context().Done(): rt.Cancel(); <-rt.Done()`:
            # whatever caused this generator to stop (client disconnect,
            # runtime finishing on its own, or the ASGI server tearing the
            # connection down), make sure the runtime is told to stop and
            # don't let the response finish until it actually has.
            disconnect_event.set()
            rt.cancel()
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
            await rt.wait_done()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


def create_app() -> Starlette:
    app = Starlette(
        routes=[
            Route("/run", run_endpoint, methods=["POST"]),
            Route("/events", events_endpoint, methods=["GET"]),
        ],
        middleware=[Middleware(ApiKeyMiddleware)],
    )
    app.state.runtime_holder = RuntimeHolder()
    return app


app = create_app()
