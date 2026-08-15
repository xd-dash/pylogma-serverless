"""Single-request, instance-local, bounded-lifetime Redis Pub/Sub runtime.

Python/asyncio port of the Go `router` package in xd-dash/logma-serverless.
It is meant to be hosted inside a Cloud Run / Cloud Functions Gen 2 HTTP
service pinned to a concurrency of 1 request per container instance. The
HTTP request that starts the runtime owns its entire lifetime: it
establishes control-plane subscriptions (``control:add``,
``control:shutdown``), lets Redis hot-load additional subscriptions into
the running container, fans every subscribed channel's messages out as one
event stream, and shuts the runtime down (ending the request) on a
``control:shutdown`` publish or client disconnect.

Go -> Python concept mapping used throughout this module:

    goroutine                  -> asyncio.Task
    go foo()                   -> asyncio.create_task(foo())
    channel (chan T)           -> asyncio.Queue
    ch <- value                -> await queue.put(value)
    <-ch                       -> await queue.get()
    select { case ... }        -> asyncio.wait({...}, return_when=FIRST_COMPLETED)
                                   (see _select2 / _select3 helpers below)
    context.Context             -> asyncio.Event (a "stop" signal) + task.cancel()
    context.WithTimeout          -> asyncio.timeout(seconds)
    atomic.Int32 (state)        -> plain attribute (safe: single-threaded event loop,
                                   claim() never awaits between check and set)
    sync.Once                   -> plain boolean flag (same single-threaded reasoning)

The single-actor ownership discipline from the Go version is preserved:
the ``_subscriptions`` dict is only ever touched from within ``run()``
(the actor loop), so no lock is needed there either -- in asyncio this
is guaranteed as long as that dict is never mutated across an ``await``
boundary from another coroutine.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional

import redis.asyncio as redis
from redis.exceptions import RedisError

logger = logging.getLogger("pylogma_serverless.runtime")

# --------------------------------------------------------------------------
# Config constants (mirrors runtime.go's const block + handlers.go's
# sseKeepAlive). Kept identical in value to the Go original.
# --------------------------------------------------------------------------

CONTROL_ADD_CHANNEL = "control:add"
CONTROL_SHUTDOWN_CHANNEL = "control:shutdown"

INPUT_BUFFER_SIZE = 64
EVENT_BUFFER_SIZE = 64

RECONNECT_MIN_DELAY = 0.5  # seconds
RECONNECT_MAX_DELAY = 30.0  # seconds
REDIS_OPERATION_TIMEOUT = 10.0  # seconds

SSE_KEEPALIVE = 15.0  # seconds (consumed by the ASGI layer, not this module)

_CONTROL_CHANNELS = frozenset({CONTROL_ADD_CHANNEL, CONTROL_SHUTDOWN_CHANNEL})


class RuntimeState(IntEnum):
    IDLE = 0
    RUNNING = 1
    DONE = 2


# --------------------------------------------------------------------------
# Message payload shapes (mirrors the JSON-tagged structs in runtime.go)
# --------------------------------------------------------------------------


@dataclass(slots=True)
class RuntimeMessage:
    """A raw message pulled off some Redis channel, forwarded worker -> actor."""

    channel: str
    payload: str


@dataclass(slots=True)
class SubscriptionStopped:
    """Reported worker -> actor when a per-channel subscription task ends."""

    channel: str


@dataclass(slots=True)
class PublishRequest:
    """The payload delivered to a subscribed data channel and re-emitted as
    an SSE event. If the message itself doesn't carry a channel, the Redis
    channel it arrived on is substituted (see handle_publish)."""

    channel: Optional[str] = None
    data: Any = None

    def to_json_dict(self) -> dict:
        out: dict = {}
        if self.channel:
            out["channel"] = self.channel
        if self.data is not None:
            out["data"] = self.data
        return out


async def _select2(fut_a: "asyncio.Future", fut_b: "asyncio.Future"):
    """select { case a; case b } for two already-scheduled awaitables.

    Returns (winner_index, result_or_exception_raised). Cancels the loser.
    Neither future is re-usable after this call.
    """
    done, pending = await asyncio.wait({fut_a, fut_b}, return_when=asyncio.FIRST_COMPLETED)
    for fut in pending:
        fut.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await fut
    if fut_a in done:
        return 0, fut_a.result()
    return 1, fut_b.result()


class Subscription:
    """Owns one Redis channel's worker task, analogous to Go's
    `*subscription` (a per-channel context.CancelFunc + goroutine)."""

    __slots__ = ("channel", "task")

    def __init__(self, channel: str, task: "asyncio.Task"):
        self.channel = channel
        self.task = task


class Runtime:
    """Single-use actor that owns a set of Redis subscriptions for the
    lifetime of one HTTP request. Mirrors Go's `*Runtime`.
    """

    def __init__(self) -> None:
        self.client: redis.Redis = redis.Redis.from_url(
            _redis_url(), password=os.environ.get("REDISCLI_AUTH") or None
        )

        self.state: RuntimeState = RuntimeState.IDLE
        self._started = False  # sync.Once equivalent for start()

        # channels -> asyncio.Queue
        self.input: "asyncio.Queue[RuntimeMessage]" = asyncio.Queue(maxsize=INPUT_BUFFER_SIZE)
        self.events: "asyncio.Queue[PublishRequest]" = asyncio.Queue(maxsize=EVENT_BUFFER_SIZE)
        self.status: "asyncio.Queue[SubscriptionStopped]" = asyncio.Queue(maxsize=INPUT_BUFFER_SIZE)

        # context.Context/CancelFunc equivalent for the whole runtime.
        self._stop_event = asyncio.Event()
        self.done_event = asyncio.Event()

        # single-actor-owned state; never mutated from another coroutine.
        self._subscriptions: dict[str, Subscription] = {}

        self._watcher_task: Optional["asyncio.Task"] = None
        self._run_task: Optional["asyncio.Task"] = None

    # -- public API -------------------------------------------------------

    def claim(self) -> bool:
        """CompareAndSwap(IDLE -> RUNNING). Safe without a lock: this method
        never awaits, and asyncio only switches coroutines at an await
        point, so no other task can interleave between the check and the
        assignment."""
        if self.state == RuntimeState.IDLE:
            self.state = RuntimeState.RUNNING
            return True
        return False

    def cancel(self) -> None:
        """Idempotent, safe to call from any task, any number of times."""
        self._stop_event.set()

    async def wait_done(self) -> None:
        await self.done_event.wait()

    async def start(self, external_disconnect: Optional[asyncio.Event] = None) -> None:
        """Equivalent of Go's Start(ctx). `external_disconnect`, if given,
        is an asyncio.Event the caller sets when the originating HTTP
        request disconnects; a watcher task forwards that into
        self.cancel(), exactly like Go's:

            select {
            case <-ctx.Done():
                rt.cancel()
            case <-rt.ctx.Done():
            }

        Guarded so a double call is a no-op, matching sync.Once.
        """
        if self._started:
            return
        self._started = True

        if external_disconnect is not None:
            self._watcher_task = asyncio.create_task(
                self._forward_external_cancel(external_disconnect)
            )

        try:
            await self._run()
        finally:
            self.state = RuntimeState.DONE
            self.done_event.set()
            if self._watcher_task is not None:
                self._watcher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._watcher_task

    async def aclose_client(self) -> None:
        """Not called anywhere by default -- mirrors the Go version, which
        never explicitly closes its redis.Client either. Exposed so a
        caller that wants stricter cleanup than the original can use it.
        """
        await self.client.aclose()

    async def _forward_external_cancel(self, external_disconnect: asyncio.Event) -> None:
        _, _ = await _select2(
            asyncio.ensure_future(external_disconnect.wait()),
            asyncio.ensure_future(self._stop_event.wait()),
        )
        self.cancel()

    # -- the actor loop -----------------------------------------------------

    async def _run(self) -> None:
        try:
            # Mandatory control-channel subscriptions first; failure here
            # aborts the whole runtime immediately (mirrors run() lines
            # 191-198 in runtime.go).
            try:
                self._start_subscription(CONTROL_ADD_CHANNEL)
                self._start_subscription(CONTROL_SHUTDOWN_CHANNEL)
            except Exception:
                logger.exception("failed to start control-channel subscriptions")
                return

            await self._bootstrap()

            input_get = asyncio.ensure_future(self.input.get())
            status_get = asyncio.ensure_future(self.status.get())
            stop_wait = asyncio.ensure_future(self._stop_event.wait())

            try:
                while True:
                    done, _pending = await asyncio.wait(
                        {input_get, status_get, stop_wait},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if stop_wait in done:
                        return

                    if input_get in done:
                        msg = input_get.result()
                        input_get = asyncio.ensure_future(self.input.get())

                        if msg.channel == CONTROL_ADD_CHANNEL:
                            self._handle_add(msg.payload)
                        elif msg.channel == CONTROL_SHUTDOWN_CHANNEL:
                            self._handle_shutdown(msg.payload)
                            return
                        else:
                            await self._handle_publish(msg.channel, msg.payload)

                    if status_get in done:
                        stopped = status_get.result()
                        status_get = asyncio.ensure_future(self.status.get())

                        sub = self._subscriptions.pop(stopped.channel, None)
                        if sub is not None:
                            with contextlib.suppress(asyncio.CancelledError):
                                await sub.task

                        # Self-healing restart, mirroring run()'s status
                        # handling: only while not shutting down, and never
                        # for the two control channels (their failure at
                        # bootstrap is treated as fatal above instead).
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
                for fut in (input_get, status_get, stop_wait):
                    if not fut.done():
                        fut.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await fut
        finally:
            await self._stop_all()

    async def _bootstrap(self) -> None:
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

    async def _handle_publish(self, channel: str, payload: str) -> None:
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

        put = asyncio.ensure_future(self.events.put(req))
        stop_wait = asyncio.ensure_future(self._stop_event.wait())
        winner, _ = await _select2(put, stop_wait)
        if winner == 1:
            # Runtime is shutting down; drop the message rather than block
            # forever waiting for room in `events`, matching handlePublish's
            # `select { case rt.events <- ...: case <-rt.ctx.Done(): }`.
            return

    # -- subscription lifecycle ----------------------------------------------

    def _start_subscription(self, channel: str) -> None:
        if channel in self._subscriptions:
            return
        task = asyncio.create_task(self._subscription_worker(channel))
        self._subscriptions[channel] = Subscription(channel=channel, task=task)

    async def _stop_all(self) -> None:
        subs = list(self._subscriptions.values())
        for sub in subs:
            sub.task.cancel()
        for sub in subs:
            with contextlib.suppress(asyncio.CancelledError):
                await sub.task
        self._subscriptions.clear()

    async def _subscription_worker(self, channel: str) -> None:
        """Analogous to Go's subscriptionWorker: (re)subscribes with
        exponential backoff on error, and forwards every message it
        receives onto self.input until cancelled.
        """
        delay = RECONNECT_MIN_DELAY
        try:
            while True:
                pubsub = self.client.pubsub()
                try:
                    async with asyncio.timeout(REDIS_OPERATION_TIMEOUT):
                        await pubsub.subscribe(channel)
                except (RedisError, TimeoutError, asyncio.TimeoutError) as exc:
                    await pubsub.aclose()
                    logger.warning("subscribe(%s) failed: %s; retrying in %.1fs", channel, exc, delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, RECONNECT_MAX_DELAY)
                    continue

                delay = RECONNECT_MIN_DELAY
                try:
                    async for message in pubsub.listen():
                        if message["type"] != "message":
                            continue
                        data = message["data"]
                        if isinstance(data, bytes):
                            data = data.decode("utf-8", errors="replace")
                        await self.input.put(RuntimeMessage(channel=channel, payload=data))
                finally:
                    await pubsub.aclose()
        except asyncio.CancelledError:
            raise
        finally:
            with contextlib.suppress(asyncio.QueueFull):
                self.status.put_nowait(SubscriptionStopped(channel=channel))


class RuntimeHolder:
    """Mints one Runtime per session and hands it to whichever request
    claims it, mirroring Go's runtimeHolder. A container instance lives
    across many sequential requests (a warm instance is reused between
    invocations), but each request's runtime is single-use: once a
    session's Runtime finishes, the next request gets a fresh one rather
    than being permanently locked out.

    An external `maxInstanceRequestConcurrency=1` (Cloud Run/Cloud
    Functions Gen 2) is what guarantees only one request -- and therefore
    only one live Runtime -- exists at a time; no lock is needed here
    because asyncio is single-threaded and claim() never awaits.
    """

    def __init__(self) -> None:
        self._runtime: Optional[Runtime] = None

    def claim(self) -> Optional[Runtime]:
        if self._runtime is None or self._runtime.state == RuntimeState.DONE:
            self._runtime = Runtime()
        if self._runtime.claim():
            return self._runtime
        return None


def _redis_url() -> str:
    uri = os.environ.get("REDIS_URI", "redis://localhost:6379")
    if "://" not in uri:
        uri = f"redis://{uri}"
    return uri
