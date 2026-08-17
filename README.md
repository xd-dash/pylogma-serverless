# pylogma-serverless (Gen 1 branch)

Threaded/synchronous port of [xd-dash/logma-serverless](https://github.com/xd-dash/logma-serverless)'s
Go `router` package: a single-request, instance-local, bounded-lifetime
Redis Pub/Sub runtime. This branch targets **Google Cloud Functions
Gen 1**, dispatched through [dash-xd/pyspace-minimal](https://github.com/dash-xd/pyspace-minimal)'s
`CloudFunctionApp` (Flask + `functions-framework`, routes hot-loaded from
a `ROUTER_MODULE`).

> This repo also has an async/Starlette/SSE branch,
> `claude/pylogma-serverless-async-zmr63w`, targeting Cloud Run / Cloud
> Functions **Gen 2**. Read the next section before picking a branch --
> the two are not interchangeable, because of a real platform
> constraint, not just a style preference.

## Why this branch has no SSE

Cloud Functions Gen 1 runs under `functions-framework`'s **synchronous
WSGI** Flask app. There is no ASGI, no `async def`, and critically:
**Gen 1 does not support streaming HTTP responses** -- Google buffers the
entire response body and sends it only once the function returns. Real
push-based SSE (`text/event-stream`, flushed chunk by chunk as messages
arrive) requires Gen 2 / Cloud Run.

So this branch drops the async branch's `GET /events` SSE endpoint
entirely and keeps only a **blocking `POST /run`**, matching the Go
version's `POST /run` behavior exactly: claim a runtime, run the Redis
actor loop synchronously until `control:shutdown` (or the platform's own
function timeout), then return `{"status": "stopped"}`. If you need
real-time push delivery to a browser or long-lived client, use the Gen 2
branch instead.

## Layout

```
main.py                          -- Gen 1 entrypoint: CloudFunctionApp(...).build() -> `main`
router.py                        -- ROUTER_MODULE target; registers POST /run directly
                                     on the shared Flask app (see its docstring for why
                                     pyspace-minimal's declarative ROUTES dict alone isn't
                                     enough here)
pylogma_serverless/
  runtime.py                     -- the Runtime actor (threaded rewrite of the Go/async version)
  views.py                       -- run_view() (Flask view function) + X-API-Key auth check
tests/                            -- unit tests for the control-message handlers and the
                                     claim()/RuntimeHolder state machine
```

## Design mapping (Go / async branch -> this branch)

| Go / asyncio branch                              | This branch (threaded)                              |
|----------------------------------------------------|--------------------------------------------------------|
| goroutine / `asyncio.Task`                          | `threading.Thread`                                     |
| `go foo()` / `asyncio.create_task(foo())`           | `threading.Thread(target=foo).start()`                 |
| `chan T` / `asyncio.Queue`                          | `queue.Queue`                                           |
| `ch <- v` / `await queue.put(v)`                    | `queue.put(v, timeout=...)`                             |
| `<-ch` / `await queue.get()`                        | `queue.get(timeout=...)`                                |
| `select { case a: ...; case b: ... }`               | `_select_get()` -- a short-timeout poll loop over each queue, since stdlib `queue.Queue` has no wait-for-any-of primitive |
| `context.Context` cancellation / `asyncio.Event`    | `threading.Event`, checked cooperatively -- see below   |
| `atomic.Int32` (state) / plain attribute            | plain attribute guarded by a real `threading.Lock` (a Flask/gunicorn worker can genuinely serve overlapping requests on separate threads, unlike a single-threaded asyncio event loop) |
| `sync.Once` / plain boolean flag                    | boolean flag guarded by a `threading.Lock`               |
| `redis.asyncio.Redis` + `pubsub.listen()`           | sync `redis.Redis` + `pubsub.get_message(timeout=...)` polled in a loop |

`pubsub.get_message(timeout=POLL_INTERVAL)` is used instead of
`pubsub.listen()` deliberately: `listen()` blocks indefinitely on the
socket, and cancelling that from another thread by calling
`pubsub.close()` cross-thread races the connection teardown against an
in-flight socket read (this was tried first and reliably produced
`AttributeError`/`ConnectionError` noise under load). `get_message` with
a timeout lets each subscription's own thread wake up on its own,
`POLL_INTERVAL` (0.2s) at a time, to check its `stop_event` -- so a
`PubSub` object is only ever touched by the one thread that owns it.

The single-actor ownership discipline from the Go version is preserved:
`Runtime._subscriptions` is only ever read or mutated from inside
`Runtime._run()`, which runs on a single thread (the one that called
`start()`) for the whole lifetime of that `Runtime`.

The control-message handling logic (`_handle_add`, `_handle_shutdown`,
`_handle_publish`'s channel-default-override and empty-payload-drop
rules) and the constants block are unchanged from the Go version and the
async branch -- none of it was async-specific.

### Fan-in topology

```
Redis
  |
  |  pubsub.get_message(timeout=0.2s), polled per channel (subscription worker threads)
  v
self.input (queue.Queue, maxsize=64)
  |
  |  single actor loop (Runtime._run), running on the /run request's own thread
  v
self.events (queue.Queue, maxsize=64)   -- populated but not drained by anything on
                                            this branch; kept for parity/future use
```

There's no consumer draining `self.events` on this branch (no SSE
endpoint), so it's populated by `_handle_publish` exactly like the async
branch, using the same drop-if-shutting-down-and-full backpressure rule,
but nothing reads it back out. If you need the accumulated events
returned to the caller, extend `run_view` to drain `rt.events` after
`rt.start()` returns and include them in the JSON response.

### Lifecycle / termination

Two ways this branch's runtime ends (one fewer than the Go version and
the async branch):

1. A `control:shutdown` message is published (payload `{"reason": "..."}`,
   reason is optional).
2. `Runtime.cancel()` is called explicitly (idempotent, safe from any
   thread, any number of times) -- not wired to anything by default on
   this branch, see below.

**Not available on this branch: client-disconnect-triggered shutdown.**
Flask/WSGI has no live request-disconnect signal the way Starlette's
`request.is_disconnected()` does, so if the HTTP client goes away mid
`/run`, the runtime keeps running regardless -- it only stops via
`control:shutdown` or the platform's own function execution timeout
(Gen 1 HTTP functions: 540s max). This is a real behavior difference
from both the Go version and the async branch; if you need
disconnect-aware cancellation, that's another reason to prefer the Gen 2
branch.

## Redis message schema

Same JSON shapes as the Go version and the async branch:

- `control:add` -> `{"channel": "some:channel:name"}` -- hot-loads a new
  subscription into the running container.
- `control:shutdown` -> `{"reason": "..."}` (optional) -- drains and
  terminates the runtime.
- Any other subscribed channel -> `{"channel": "override (optional)", "data": <any JSON>}`.
  If `channel` is omitted, the Redis channel the message physically
  arrived on is substituted. An empty payload or literal `{}` is silently
  dropped. Payloads that fail to parse as JSON are logged and dropped,
  never fatal to the runtime.

## Config (env vars)

| Variable                      | Purpose                                                      |
|--------------------------------|---------------------------------------------------------------|
| `REDIS_URI`                    | Redis address, e.g. `redis://localhost:6379`                 |
| `REDISCLI_AUTH`                 | Redis password (optional)                                    |
| `REDIS_DEFAULT_SUBSCRIPTIONS`   | JSON array of channel names to subscribe at boot, e.g. `["a","b"]` |
| `API_KEY`                       | Required; requests must send a matching `X-API-Key` header, compared in constant time |
| `ROUTER_MODULE`                 | Required by pyspace-minimal's `CloudFunctionApp`; set to `router` so it imports this repo's `router.py` |

Constants (`pylogma_serverless/runtime.py`), identical values to the Go
`const` block and the async branch:

```
CONTROL_ADD_CHANNEL      = "control:add"
CONTROL_SHUTDOWN_CHANNEL = "control:shutdown"
INPUT_BUFFER_SIZE        = 64
EVENT_BUFFER_SIZE        = 64
RECONNECT_MIN_DELAY      = 0.5s
RECONNECT_MAX_DELAY      = 30s
REDIS_OPERATION_TIMEOUT  = 10s
POLL_INTERVAL            = 0.2s   -- new on this branch; see design mapping above
```

## Routes

- `POST /run` -- claims a runtime and blocks until it stops. Returns
  `200 {"status": "stopped"}`, or `409` if a runtime is already claimed,
  or `401` for a missing/incorrect `X-API-Key` header.

Registered directly on the shared Flask app rather than through
pyspace-minimal's `ROUTES` dict, because `CloudFunctionApp.register_routes()`
calls `add_url_rule(rule, endpoint=rule, view_func=view_func)` without a
`methods=` argument, defaulting to GET/HEAD/OPTIONS only -- see
`router.py`'s docstring for the full explanation. No `/` health-check
route, matching the Go version's reasoning about not wasting the
container's request-handling slot.

## Running locally

Requires Python 3.12 (pyspace-minimal's `cloud_function_app` package
pins `requires-python = ">=3.12"`, and it's also the Gen 1 deploy
runtime this branch targets).

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

export API_KEY=dev-key
export REDIS_URI=redis://localhost:6379
export ROUTER_MODULE=router

functions-framework --target=main --source=main.py --port 8080
```

```bash
# in another shell -- /run blocks until control:shutdown is published
curl -X POST -H "X-API-Key: dev-key" http://localhost:8080/run &

# in a third shell
redis-cli publish control:add '{"channel":"dev:global:logs:1"}'
redis-cli publish dev:global:logs:1 '{"data":{"msg":"hello"}}'
redis-cli publish control:shutdown '{"reason":"done"}'
# the first curl now returns {"status": "stopped"}
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Covers `_handle_publish`'s channel-default-override and empty/`{}`-payload
drop, `_handle_add`'s dedup/validation, and `claim()`/`RuntimeHolder`'s
compare-and-swap state machine -- ported from the Go version's
`runtime_test.go` cases, adapted to synchronous calls (no
`pytest-asyncio` needed on this branch). These don't require a running
Redis server since `Runtime`'s constructor is lazy about connecting.

## Deploying

```bash
gcloud functions deploy pylogma-serverless \
  --no-gen2 \
  --runtime python312 \
  --entry-point main \
  --trigger-http \
  --set-env-vars ROUTER_MODULE=router,REDIS_URI=...,REDISCLI_AUTH=...,API_KEY=...
```

`--no-gen2` forces Gen 1, matching pyspace-minimal's own README. The
deployed source directory needs `main.py`, `router.py`,
`pylogma_serverless/`, and `requirements.txt` together -- pip can't reach
into a separately-deployed package.

## Known gaps vs. the Go version / the async branch

- No client-disconnect cancellation (see Lifecycle/termination above).
- No SSE / real-time event delivery (see "Why this branch has no SSE"
  above); `Runtime.events` is populated but has no consumer wired up by
  default.
- Same as the Go version: no panic/exception-recovery safety net around
  background work. An unexpected exception in a subscription worker
  thread is caught, logged, and reported to the actor (which restarts
  that subscription unless the runtime is shutting down); an unexpected
  exception in the actor loop itself (`Runtime._run`) is not recovered
  from -- it propagates out of `Runtime.start()` to the Flask request
  thread that called it, surfacing as a 500.
