"""Single-request, instance-local, bounded-lifetime Redis Pub/Sub runtime.

Threaded/synchronous port of the Go `router` package in
xd-dash/logma-serverless, for hosting on Google Cloud Functions **Gen 1**
via `functions-framework` (synchronous WSGI/Flask -- see app.py's module
docstring for the routing side of this).

Gen 1 has no ASGI, no `async def`, and no support for long-lived
streaming HTTP responses (the platform buffers the whole response body),
so this is not a drop-in reuse of the asyncio version in this repo's
`claude/pylogma-serverless-async-zmr63w` branch -- it's a parallel
implementation using the thread/queue equivalents of the same design:

    goroutine                   -> threading.Thread
    go foo()                    -> threading.Thread(target=foo).start()
    channel (chan T)            -> queue.Queue
    ch <- value                 -> queue.put(value)
    <-ch                        -> queue.get()
    select { case ... }         -> a short-timeout poll loop over each
                                    queue.get(timeout=...), since stdlib
                                    Queue has no wait-for-any-of primitive
                                    (see _select_get below)
    context.Context cancellation -> threading.Event (a stop signal) +
                                    pubsub.close() to unblock a thread
                                    parked in pubsub.listen()
    atomic.Int32 (state)        -> plain attribute guarded by a
                                    threading.Lock (unlike the asyncio
                                    version, a Flask/gunicorn worker can
                                    genuinely serve overlapping requests
                                    on separate threads, so this needs a
                                    real lock)
    sync.Once                   -> threading.Lock guarding a boolean flag

The control-message handling logic (_handle_add, _handle_shutdown,
_handle_publish's channel-default-override and empty-payload-drop rules)
is unchanged from the asyncio version -- none of it was async-specific.

Known behavior difference from both the Go version and the asyncio
branch: Flask/WSGI has no live request-disconnect signal (unlike
Starlette's `request.is_disconnected()`), so this runtime does not
terminate on client disconnect. Its only termination paths are a
`control:shutdown` publish and the platform's own function execution
timeout (Gen 1 HTTP functions: 540s max).
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Optional

import redis
from redis.exceptions import RedisError

logger = logging.getLogger("pylogma_serverless.runtime")

# --------------------------------------------------------------------------
# Config constants -- identical values to the Go original and to this
# repo's asyncio branch. SSE_KEEPALIVE is not used here (no streaming
# endpoint on Gen 1) but is kept for reference/parity.
# --------------------------------------------------------------------------

CONTROL_ADD_CHANNEL = "control:add"
CONTROL_SHUTDOWN_CHANNEL = "control:shutdown"

INPUT_BUFFER_SIZE = 64
EVENT_BUFFER_SIZE = 64

RECONNECT_MIN_DELAY = 0.5  # seconds
RECONNECT_MAX_DELAY = 30.0  # seconds
REDIS_OPERATION_TIMEOUT = 10.0  # seconds

# How often the actor loop's poll-based "select" and the subscription
# workers' stop-check wake up. Small enough that shutdown/cancel feel
# immediate; large enough not to spin the CPU.
POLL_INTERVAL = 0.2  # seconds

_CONTROL_CHANNELS = frozenset({CONTROL_ADD_CHANNEL, CONTROL_SHUTDOWN_CHANNEL})


class RuntimeState(IntEnum):
    IDLE = 0
    RUNNING = 1
    DONE = 2


# --------------------------------------------------------------------------
# Message payload shapes (same JSON wire schema as the Go version and the
# asyncio branch)
# --------------------------------------------------------------------------


@dataclass(slots=True)
class RuntimeMessage:
    """A raw message pulled off some Redis channel, forwarded worker -> actor."""

    channel: str
    payload: str


@dataclass(slots=True)
class SubscriptionStopped:
    """Reported worker -> actor when a per-channel subscription thread ends."""

    channel: str


@dataclass(slots=True)
class PublishRequest:
    """The payload produced for a subscribed data channel. If the message
    itself doesn't carry a channel, the Redis channel it arrived on is
    substituted (see _handle_publish)."""

    channel: Optional[str] = None
    data: Any = None

    def to_json_dict(self) -> dict:
        out: dict = {}
        if self.channel:
            out["channel"] = self.channel
        if self.data is not None:
            out["data"] = self.data
        return out


_SENTINEL = object()


def _select_get(*queues: "queue.Queue", stop_event: threading.Event):
    """select { case <-q1: ...; case <-q2: ...; case <-ctx.Done(): ... }
    for stdlib queue.Queue, which has no native wait-for-any-of. Polls
    each queue with a short non-blocking-ish timeout, returning as soon
    as any one has an item or stop_event fires.

    Returns (queue, item) for whichever queue produced first, or
    (None, None) if stop_event fired first.
    """
    while not stop_event.is_set():
        for q in queues:
            try:
                item = q.get(timeout=POLL_INTERVAL / len(queues))
            except queue.Empty:
                continue
            return q, item
    return None, None


class Subscription:
    """Owns one Redis channel's worker thread plus its own stop signal,
    analogous to Go's `*subscription` (a per-channel context.CancelFunc +
    goroutine).

    Note there is no reference to the worker's live PubSub object here:
    redis-py's PubSub is not safe to call `.close()` on from a thread
    other than the one that's blocked reading from it (doing so races
    the connection teardown against an in-flight socket read). So
    cancellation is cooperative only -- stop_event is the only thing
    touched cross-thread; the worker itself is responsible for noticing
    it and closing its own PubSub (see Runtime._subscription_worker).
    """

    __slots__ = ("channel", "thread", "stop_event")

    def __init__(self, channel: str, thread: threading.Thread, stop_event: threading.Event):
        self.channel = channel
        self.thread = thread
        self.stop_event = stop_event


class Runtime:
    """Single-use actor that owns a set of Redis subscriptions for the
    lifetime of one HTTP request. Mirrors Go's `*Runtime`, but the actor
    loop itself runs synchronously on the calling (request) thread rather
    than as a background task -- see start().
    """

    def __init__(self) -> None:
        self.client: redis.Redis = redis.Redis.from_url(
            _redis_url(), password=os.environ.get("REDISCLI_AUTH") or None
        )

        self.state: RuntimeState = RuntimeState.IDLE
        self._claim_lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._started = False

        self.input: "queue.Queue[RuntimeMessage]" = queue.Queue(maxsize=INPUT_BUFFER_SIZE)
        self.events: "queue.Queue[PublishRequest]" = queue.Queue(maxsize=EVENT_BUFFER_SIZE)
        self.status: "queue.Queue[SubscriptionStopped]" = queue.Queue(maxsize=INPUT_BUFFER_SIZE)

        self._stop_event = threading.Event()
        self._done_event = threading.Event()

        # single-actor-owned state; only ever touched from _run(), which
        # runs on a single thread for this Runtime's whole lifetime.
        self._subscriptions: dict[str, Subscription] = {}

    # -- public API -------------------------------------------------------

    def claim(self) -> bool:
        """CompareAndSwap(IDLE -> RUNNING)."""
        with self._claim_lock:
            if self.state == RuntimeState.IDLE:
                self.state = RuntimeState.RUNNING
                return True
            return False

    def cancel(self) -> None:
        """Idempotent, safe to call from any thread, any number of times."""
        self._stop_event.set()

    def wait_done(self, timeout: Optional[float] = None) -> bool:
        return self._done_event.wait(timeout=timeout)

    def start(self) -> None:
        """Equivalent of Go's Start(ctx), minus the ctx-forwarding watcher
        (Flask/WSGI has no live disconnect signal to forward -- see the
        module docstring). Runs the actor loop on the calling thread, so
        this call blocks until the runtime stops. Guarded so a double
        call is a no-op, matching sync.Once.
        """
        with self._start_lock:
            if self._started:
                return
            self._started = True

        try:
            self._run()
        finally:
            self.state = RuntimeState.DONE
            self._done_event.set()

    def close_client(self) -> None:
        """Not called anywhere by default -- mirrors the Go version, which
        never explicitly closes its redis.Client either."""
        self.client.close()

    # -- the actor loop -----------------------------------------------------

    def _run(self) -> None:
        try:
            try:
                self._start_subscription(CONTROL_ADD_CHANNEL)
                self._start_subscription(CONTROL_SHUTDOWN_CHANNEL)
            except Exception:
                logger.exception("failed to start control-channel subscriptions")
                return

            self._bootstrap()

            while True:
                q, item = _select_get(self.input, self.status, stop_event=self._stop_event)

                if q is None:
                    return

                if q is self.input:
                    msg: RuntimeMessage = item
                    if msg.channel == CONTROL_ADD_CHANNEL:
                        self._handle_add(msg.payload)
                    elif msg.channel == CONTROL_SHUTDOWN_CHANNEL:
                        self._handle_shutdown(msg.payload)
                        return
                    else:
                        self._handle_publish(msg.channel, msg.payload)

                elif q is self.status:
                    stopped: SubscriptionStopped = item
                    sub = self._subscriptions.pop(stopped.channel, None)
                    if sub is not None:
                        sub.thread.join(timeout=REDIS_OPERATION_TIMEOUT)

                    if (
                        not self._stop_event.is_set()
                        and stopped.channel not in _CONTROL_CHANNELS
                    ):
                        logger.warning(
                            "subscription %s terminated unexpectedly; restarting",
                            stopped.channel,
                        )
                        self._start_subscription(stopped.channel)
        finally:
            self._stop_all()

    def _bootstrap(self) -> None:
        raw = os.environ.get("REDIS_DEFAULT_SUBSCRIPTIONS")
        if not raw:
            return
        try:
            channels = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("REDIS_DEFAULT_SUBSCRIPTIONS is not valid JSON: %r", raw)
            return
        if not isinstance(channels, list):
            logger.error("REDIS_DEFAULT_SUBSCRIPTIONS must be a JSON array of strings")
            return
        for channel in channels:
            if not isinstance(channel, str) or not channel:
                continue
            self._start_subscription(channel)

    # -- control-message handlers --------------------------------------------

    def _handle_add(self, payload: str) -> None:
        try:
            obj = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            logger.error("control:add payload is not valid JSON: %r", payload)
            return
        channel = obj.get("channel") if isinstance(obj, dict) else None
        if not channel:
            logger.error("control:add payload missing channel: %r", payload)
            return
        if channel in self._subscriptions:
            return
        self._start_subscription(channel)

    def _handle_shutdown(self, payload: str) -> None:
        reason = ""
        if payload and payload != "{}":
            try:
                obj = json.loads(payload)
                if isinstance(obj, dict):
                    reason = obj.get("reason", "")
            except json.JSONDecodeError:
                logger.error("control:shutdown payload is not valid JSON: %r", payload)
        logger.info("runtime shutting down: reason=%r", reason)

    def _handle_publish(self, channel: str, payload: str) -> None:
        if not payload or payload == "{}":
            return
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            logger.error("message on %s is not valid JSON: %r", channel, payload)
            return
        if not isinstance(obj, dict):
            logger.error("message on %s is not a JSON object: %r", channel, payload)
            return

        req = PublishRequest(channel=obj.get("channel") or None, data=obj.get("data"))
        if not req.channel:
            req.channel = channel

        # select { case rt.events <- req: case <-rt.ctx.Done(): } --
        # don't block forever waiting for room in `events` if the
        # runtime is shutting down; drop the message instead.
        while not self._stop_event.is_set():
            try:
                self.events.put(req, timeout=POLL_INTERVAL)
                return
            except queue.Full:
                continue

    # -- subscription lifecycle ----------------------------------------------

    def _start_subscription(self, channel: str) -> None:
        if channel in self._subscriptions:
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._subscription_worker,
            args=(channel, stop_event),
            name=f"pylogma-sub-{channel}",
            daemon=True,
        )
        self._subscriptions[channel] = Subscription(channel=channel, thread=thread, stop_event=stop_event)
        thread.start()

    def _stop_all(self) -> None:
        subs = list(self._subscriptions.values())
        for sub in subs:
            sub.stop_event.set()
        for sub in subs:
            sub.thread.join(timeout=REDIS_OPERATION_TIMEOUT)
        self._subscriptions.clear()

    def _subscription_worker(self, channel: str, stop_event: threading.Event) -> None:
        """Analogous to Go's subscriptionWorker: (re)subscribes with
        exponential backoff on error, and forwards every message it
        receives onto self.input until stop_event fires.

        Uses `pubsub.get_message(timeout=...)` rather than
        `pubsub.listen()` (which blocks indefinitely on the socket) so
        this thread wakes up on its own, at POLL_INTERVAL cadence, to
        check stop_event -- the sync equivalent of the asyncio branch's
        `select { case <-pubsub.Channel(): case <-ctx.Done(): }`. This
        also means the PubSub object is only ever touched by the thread
        that owns it: no other thread calls .close() on it, which
        avoids racing an in-flight socket read against connection
        teardown.
        """
        delay = RECONNECT_MIN_DELAY
        try:
            while not stop_event.is_set():
                pubsub = self.client.pubsub()
                try:
                    pubsub.subscribe(channel)
                except RedisError as exc:
                    pubsub.close()
                    logger.warning(
                        "subscribe(%s) failed: %s; retrying in %.1fs", channel, exc, delay
                    )
                    if stop_event.wait(timeout=delay):
                        return
                    delay = min(delay * 2, RECONNECT_MAX_DELAY)
                    continue

                delay = RECONNECT_MIN_DELAY
                try:
                    while not stop_event.is_set():
                        try:
                            message = pubsub.get_message(
                                ignore_subscribe_messages=True, timeout=POLL_INTERVAL
                            )
                        except RedisError as exc:
                            if not stop_event.is_set():
                                logger.warning(
                                    "get_message(%s) failed: %s; reconnecting", channel, exc
                                )
                            break
                        if message is None:
                            continue
                        data = message["data"]
                        if isinstance(data, bytes):
                            data = data.decode("utf-8", errors="replace")
                        while not stop_event.is_set():
                            try:
                                self.input.put(
                                    RuntimeMessage(channel=channel, payload=data),
                                    timeout=POLL_INTERVAL,
                                )
                                break
                            except queue.Full:
                                continue
                finally:
                    pubsub.close()

                if stop_event.is_set():
                    return
                # get_message() broke out of its loop due to a RedisError
                # rather than stop_event -- the connection was lost;
                # reconnect after a short backoff, mirroring the Go
                # worker's outer retry loop.
                if stop_event.wait(timeout=RECONNECT_MIN_DELAY):
                    return
        finally:
            try:
                self.status.put_nowait(SubscriptionStopped(channel=channel))
            except queue.Full:
                logger.error("status queue full; dropped stop notice for %s", channel)


class RuntimeHolder:
    """Mints one Runtime per session and hands it to whichever request
    claims it, mirroring Go's runtimeHolder and this repo's asyncio
    branch. Unlike the asyncio branch, this one genuinely needs a lock:
    a Flask/gunicorn worker process can serve overlapping requests on
    separate threads, and this module-level holder is constructed once
    at cold start and reused across every warm invocation.
    """

    def __init__(self) -> None:
        self._runtime: Optional[Runtime] = None
        self._lock = threading.Lock()

    def claim(self) -> Optional[Runtime]:
        with self._lock:
            if self._runtime is None or self._runtime.state == RuntimeState.DONE:
                self._runtime = Runtime()
            rt = self._runtime
        if rt.claim():
            return rt
        return None


def _redis_url() -> str:
    uri = os.environ.get("REDIS_URI", "redis://localhost:6379")
    if "://" not in uri:
        uri = f"redis://{uri}"
    return uri
