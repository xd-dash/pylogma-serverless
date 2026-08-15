# pylogma-serverless

Python/asyncio port of [xd-dash/logma-serverless](https://github.com/xd-dash/logma-serverless)'s
Go `router` package: a single-request, instance-local, bounded-lifetime
Redis Pub/Sub runtime, exposed over Server-Sent Events (SSE).

It is meant to be hosted inside a Cloud Run / Cloud Functions Gen 2 HTTP
service pinned to `--concurrency=1`. The HTTP request that starts the
runtime owns its entire lifetime: it establishes control-plane
subscriptions (`control:add`, `control:shutdown`), lets Redis hot-load
additional subscriptions into the running container, fans every
subscribed channel's messages out as one event stream, and shuts the
runtime down (ending the request) on a `control:shutdown` publish or
client disconnect.

## Layout

```
pylogma_serverless/
  runtime.py   -- the Runtime actor (equivalent of runtime.go)
  app.py       -- Starlette app: /run, /events, API-key auth (equivalent
                  of router.go + handlers.go)
main.py        -- ASGI entrypoint (`uvicorn main:app`)
tests/         -- unit tests for the control-message handlers and the
                  claim()/RuntimeHolder state machine
```

## Design mapping (Go -> Python)

The write-up below is the reasoning this port follows, condensed to what
actually landed in code:

| Go                                   | Python                                            |
|---------------------------------------|----------------------------------------------------|
| goroutine                             | `asyncio.Task`                                     |
| `go foo()`                            | `asyncio.create_task(foo())`                       |
| `chan T`                              | `asyncio.Queue`                                    |
| `ch <- v`                             | `await queue.put(v)`                               |
| `<-ch`                                | `await queue.get()`                                |
| `select { case a: ...; case b: ... }` | `asyncio.wait({...}, return_when=FIRST_COMPLETED)` |
| `context.Context` cancellation        | `asyncio.Event` (a stop signal) + `task.cancel()`  |
| `context.WithTimeout`                 | `asyncio.timeout(seconds)`                         |
| `atomic.Int32` (state)                | plain attribute -- safe because asyncio is single-threaded and `claim()` never awaits between the check and the write |
| `sync.Once`                           | plain boolean flag, same single-threaded reasoning |
| `redis.NewClient` + `.Subscribe`      | `redis.asyncio.Redis` + `pubsub()` / `async for message in pubsub.listen()` |

`redis.asyncio` (`redis-py`'s native asyncio client) is the async Redis
client; `pubsub.listen()` is an async generator that yields control back
to the event loop whenever there's no message, so other tasks (other
subscriptions, the actor loop, the HTTP handler) keep running while it
waits -- the same "cooperative yield while blocked" property Go gets from
a goroutine blocked on `<-ch`.

The single-actor ownership discipline from the Go version is preserved:
`Runtime._subscriptions` is only ever read or mutated from inside
`Runtime._run()` (the actor loop) -- never across an `await` from another
coroutine -- so, exactly as in the Go version, no lock is needed to guard
it.

### Fan-in / fan-out topology

Same two-hop shape as the Go version:

```
Redis
  |
  |  pubsub.listen() per channel (subscription worker tasks)
  v
self.input (asyncio.Queue, maxsize=64)
  |
  |  single actor task (Runtime._run)
  v
self.events (asyncio.Queue, maxsize=64)
  |
  |  SSE generator in app.events_endpoint
  v
HTTP response (text/event-stream)
```

- One `asyncio.Task` per subscribed Redis channel (`Runtime._subscription_worker`),
  minimum two for the mandatory control channels. Each one resubscribes
  with exponential backoff (500ms -> 30s cap) on error, exactly like
  `subscriptionWorker` in the Go version.
- A single actor task (`Runtime._run`) owns the `_subscriptions` map,
  reads off `self.input`/`self.status`, routes `control:add` /
  `control:shutdown` messages, and forwards everything else to
  `self.events` as a `PublishRequest`.
- The ASGI request handler for `/events` reads `self.events` and writes
  each one as an SSE frame (`event: message\ndata: <json>\n\n`), with a
  15-second keepalive comment (`: keepalive\n\n`) when the stream is
  otherwise idle -- identical cadence to the Go version's `sseKeepAlive`.

### Lifecycle / termination

Exactly three ways a runtime ends, mirroring the Go version:

1. A `control:shutdown` message is published (payload `{"reason": "..."}`,
   reason is optional).
2. The client disconnects. Starlette has no push-based disconnect event,
   so `app._watch_disconnect` polls `request.is_disconnected()` once a
   second and sets a stop `asyncio.Event` when it fires -- the poll-based
   equivalent of Go's `select { case <-r.Context().Done(): ... }`.
3. `Runtime.cancel()` is called explicitly (idempotent, safe from any
   task, any number of times).

There is no idle/inactivity timeout on the runtime itself, same as the Go
version -- session lifetime is unbounded until one of the three signals
above fires (bounded in production by the platform's own request
timeout).

## Redis message schema

Same JSON shapes as the Go version:

- `control:add` -> `{"channel": "some:channel:name"}` -- hot-loads a new
  subscription into the running container.
- `control:shutdown` -> `{"reason": "..."}` (optional) -- drains and
  terminates the runtime.
- Any other subscribed channel -> `{"channel": "override (optional)", "data": <any JSON>}`.
  If `channel` is omitted, the Redis channel the message physically
  arrived on is substituted. An empty payload or literal `{}` is silently
  dropped (no SSE event emitted). Payloads that fail to parse as JSON are
  logged and dropped, never fatal to the runtime.

## Config (env vars)

Same names and defaults as the Go version:

| Variable                      | Purpose                                                      |
|--------------------------------|---------------------------------------------------------------|
| `REDIS_URI`                    | Redis address, e.g. `redis://localhost:6379`                 |
| `REDISCLI_AUTH`                 | Redis password (optional)                                    |
| `REDIS_DEFAULT_SUBSCRIPTIONS`   | JSON array of channel names to subscribe at boot, e.g. `["a","b"]` |
| `API_KEY`                       | Required; requests must send a matching `X-API-Key` header, compared in constant time |

Constants (`pylogma_serverless/runtime.py`), identical values to the Go
`const` block:

```
CONTROL_ADD_CHANNEL      = "control:add"
CONTROL_SHUTDOWN_CHANNEL = "control:shutdown"
INPUT_BUFFER_SIZE        = 64
EVENT_BUFFER_SIZE        = 64
RECONNECT_MIN_DELAY      = 0.5s
RECONNECT_MAX_DELAY      = 30s
REDIS_OPERATION_TIMEOUT  = 10s
SSE_KEEPALIVE            = 15s
```

## Routes

- `POST /run` -- claims a runtime and blocks until it stops (no SSE).
  Returns `200 {"status": "stopped"}`, or `409` if a runtime is already
  claimed.
- `GET /events` -- claims a runtime, starts it in the background, and
  streams `text/event-stream` until it stops or the client disconnects.
  `409` if already claimed.

Deliberately no `/` health-check route: with `--concurrency=1`, a health
check would consume the container's only request slot, same reasoning as
the Go version.

## Running locally

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

export API_KEY=dev-key
export REDIS_URI=redis://localhost:6379

uvicorn main:app --host 0.0.0.0 --port 8080
```

```bash
# in another shell
curl -N -H "X-API-Key: dev-key" http://localhost:8080/events

# in a third shell
redis-cli publish control:add '{"channel":"dev:global:logs:1"}'
redis-cli publish dev:global:logs:1 '{"data":{"msg":"hello"}}'
redis-cli publish control:shutdown '{"reason":"done"}'
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Covers `handle_publish`'s channel-default-override and empty/`{}`-payload
drop, `handle_add`'s dedup/validation, and `claim()`/`RuntimeHolder`'s
compare-and-swap state machine -- ported from the Go version's
`runtime_test.go` cases. These don't require a running Redis server since
`Runtime`'s constructor is lazy about connecting.

## Deploying

Cloud Run:

```bash
gcloud run deploy pylogma-serverless \
  --source . \
  --concurrency=1 \
  --set-env-vars REDIS_URI=...,REDISCLI_AUTH=...,API_KEY=...
```

`--concurrency=1` is the Python equivalent of the Go version's
`maxInstanceRequestConcurrency=1`: it guarantees only one HTTP request --
and therefore only one live `Runtime` -- exists per container instance at
a time, which is what lets `RuntimeHolder.claim()` avoid needing a lock.

## Known gap vs. the Go version

The Go version has no panic-recovery safety net around its background
goroutines (`subscriptionWorker`, the actor loop) -- only the HTTP
handler chain is wrapped in `middleware.Recoverer`. This port mirrors
that: an unexpected exception in a subscription worker is caught, logged,
and reported to the actor (which will restart that subscription unless
the runtime is shutting down), but an unexpected exception in the actor
loop itself (`Runtime._run`) is not recovered from -- it propagates out of
`Runtime.start()` to whichever task awaited it.
