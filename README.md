# go-s3-server

Minimal S3-compatible server backed by the local filesystem. Designed as a shared build cache for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain).

## Features

- **S3 API subset** — `GetObject`, `PutObject`, `DeleteObject` (idempotent, returns `204`; the surgical lever for evicting a single poisoned cache entry without a whole-cache version-bump purge)
- **Cache-key index** — `GET /<bucket>/_index` returns a precomputed binary blob (GBCI v1) of every cacheprog action-ID hash, with strong ETag and `If-None-Match` 304 support
- **HTTP Basic Auth** — multiple users, or explicitly disable with `disable_auth: true`
- **Write-once mode** — deny overwriting existing keys with configurable conflict notification (ideal for content-addressable caches)
- **Sharded storage** — keys are automatically split into a two-level directory tree to avoid huge flat directories
- **Streaming, OOM-safe under load** — object bodies are streamed straight to/from disk on GET, PUT, and batch GET, so the server never buffers whole objects in memory. A concurrency limit sheds excess load with `503 + Retry-After` instead of queueing until it OOMs (which a fronting proxy would surface as a `502`). See [Behavior under load](#behavior-under-load).
- **Graceful drain on shutdown** — on `SIGTERM`/`SIGINT` the server stops accepting new requests and lets in-flight ones finish (up to a drain timeout) before exiting, so a rolling update never cuts off an in-progress CI upload or download. An unauthenticated `GET /_health` probe returns `200` when ready and `503` while draining. See [Graceful shutdown & rolling updates](#graceful-shutdown--rolling-updates).
- **Multi-arch Docker image** — `linux/amd64` and `linux/arm64` published to `ghcr.io/wow-look-at-my/go-s3-server`

## Quick start

Create a JSON config file:

```json
{
  "listen": ":9000",
  "bucket": "my-cache",
  "data_dir": "/var/data/s3",
  "write_once": {"action": "deny", "notification": "content_differs"},
  "credentials": [
    {"username": "alice", "password": "secret1"},
    {"username": "bob", "password": "secret2"}
  ]
}
```

Run the server:

```
go-s3-server --config config.json
```

### CLI flags

| Flag | Description |
|------|-------------|
| `--config` | Path to JSON config file (required) |
| `--listen` | Override listen address |
| `--bucket` | Override bucket name |
| `--data-dir` | Override data directory |

All flags except `--config` override the corresponding config file value.

## Config reference

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `listen` | string | `:9000` | no | Address to listen on |
| `bucket` | string | — | yes | S3 bucket name to serve |
| `data_dir` | string | — | yes | Directory to store objects |
| `write_once` | object | `{"action":"allow"}` | no | Write-once behavior (see below) |
| `disable_auth` | bool | `false` | no | If `true`, accept all requests without authentication. Must be set explicitly; `credentials` must be omitted when this is `true`. |
| `credentials` | array | — | yes (unless `disable_auth: true`) | One or more `username`/`password` pairs. Both fields must be non-empty. |
| `max_concurrent_requests` | int | `128` | no | Max in-flight requests; excess is shed with `503 + Retry-After`. `0` → default. |
| `max_object_bytes` | int | `1073741824` (1 GiB) | no | Max single PUT body; larger uploads get `413`. The body is streamed to disk, so this guards disk, not memory. `0` → default. |

### `write_once` options

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `action` | `allow`, `deny` | `allow` | Whether to allow overwriting existing keys |
| `notification` | `never`, `always`, `content_differs` | `never` | When to return HTTP 409 on overwrite attempts |

- `action: "deny"` + `notification: "never"` — silently skip overwrites (200 response)
- `action: "deny"` + `notification: "always"` — reject any overwrite attempt (409 response)
- `action: "deny"` + `notification: "content_differs"` — reject only when content differs; same content is idempotent (ideal for content-addressable caches)

## Authentication

HTTP Basic Auth. Configure one or more users in the `credentials` array:

```bash
curl -u alice:secret1 -X PUT --data-binary @file.bin http://localhost:9000/my-cache/path/to/key
curl -u alice:secret1 http://localhost:9000/my-cache/path/to/key
curl -u alice:secret1 -X DELETE http://localhost:9000/my-cache/path/to/key   # evict one entry (e.g. a poisoned key)
```

To disable auth (e.g. behind a reverse proxy that handles it), set `disable_auth: true` and omit `credentials`:

```json
{
  "bucket": "my-cache",
  "data_dir": "/var/data/s3",
  "disable_auth": true
}
```

Empty strings in a `credentials` entry are a config error — you must opt into unauthenticated operation explicitly.

### Environment variable references

Any string config value can reference an environment variable instead of being hardcoded:

```json
"credentials": [
  {
    "username": {"type": "envvar", "name": "S3_USERNAME"},
    "password": {"type": "envvar", "name": "S3_PASSWORD"}
  }
]
```

The env var is resolved at config load time.

## Audit metadata on uploads

Every `PutObject` request records the following fields as extended attributes
on the stored file (namespace `user.s3audit.*` on Unix, `.audit` sidecar on
Windows):

| Attribute | Source |
|-----------|--------|
| `uploader` | Authenticated username (or `<ANON>` when `disable_auth: true`) |
| `uploaded_at` | Server wall clock at request start (RFC 3339 nano) |
| `client_ip` | `CF-Connecting-IP` → `X-Real-IP` → first `X-Forwarded-For` → TCP peer |
| `user_agent` | Request `User-Agent` header |
| `content_length` | Size in bytes of the uploaded body |

Inspect on Linux with `getfattr -d path/to/object`. These fields exist so a
suspected compromise can be investigated without guesswork.

The same fields are written to the server's request log on every request.

## Cache version

The server stamps each `data_dir` with a cache version marker
(`.cache_version`). On startup, if the stored version does not match the
server's current version, the entire `data_dir` is wiped before serving any
requests. This is a one-way safety net: when the maintainers ship a fix that
should invalidate previously-stored content (for example, after closing a
vulnerability that could let an attacker populate the cache), they bump the
version. Next restart on every deployment forces the cache to be rebuilt from
trusted inputs.

A corrupt or unparseable marker file is a startup error — fix it manually,
don't leave it to a silent purge.

## Behavior under load

This server is built to absorb the concurrent load of a parallel CI matrix
(many jobs each batch-fetching and uploading hundreds of content-addressed
keys) without OOM-ing or returning `502`s:

- **Bodies are streamed, never buffered.** `GetObject`, `PutObject`, and the
  `_batch/get` tar response copy object bytes directly between disk and the
  socket with a fixed-size buffer. A batch of hundreds of large objects holds
  only one body in flight at a time, so memory stays flat regardless of how
  many objects (or how large) are requested concurrently.
- **Backpressure, not collapse.** At most `max_concurrent_requests` are served
  at once; further requests are shed immediately with `503 Service Unavailable`
  and a `Retry-After` header — a signal clients back off on. The server never
  queues unbounded work until the process is OOM-killed (the failure a fronting
  proxy reports as a `502`). Overload-shed requests are counted in the
  `s3_http_rejected_total` metric.
- **Bounded requests.** A single PUT is capped at `max_object_bytes` (`413` over
  the limit); a `_batch/get` is capped at 4096 keys (`400` over the limit).
- **Timeouts.** The HTTP server sets `ReadHeaderTimeout` (slowloris guard) plus
  generous `Read`/`Write`/`Idle` timeouts so a stuck connection cannot pin a
  concurrency slot indefinitely.
- **Observability.** When `--metrics-listen` is set, `/metrics` exposes request,
  storage, in-flight, and rejection counters alongside the standard Go runtime
  and process collectors (`go_memstats_*`, `process_resident_memory_bytes`,
  `go_goroutines`) — enough to see saturation and memory pressure directly.

## Graceful shutdown & rolling updates

The server drains in-flight requests instead of dropping them when told to stop,
so a rolling deploy (or any `docker stop`) does not cut off an in-progress CI
upload or batch download.

- **`GET /_health`** — an unauthenticated readiness probe. Returns `200` with
  body `ok` while serving, and `503` (with `Retry-After`) once a shutdown has
  begun. It is answered *before* authentication and admission control, so it
  needs no S3 credentials and is never shed under load — point your reverse
  proxy and orchestrator at it.
- **Drain on signal.** On `SIGTERM`/`SIGINT` the server flips `/_health` to
  `503` (so traffic routes away from this instance) and then waits for in-flight
  requests to finish — up to a 280s drain timeout, kept under a typical
  container stop grace period — before exiting. New connections are refused
  immediately; established requests are allowed to complete.

### Rolling update with docker-updater

[`docker-updater`](https://github.com/wow-look-at-my/docker-updater) performs a
zero-downtime update against this drain: it starts the replacement, waits for the
new instance's `/_health` to go green, then stops the old one with a grace
period long enough for it to drain. Label the container:

```yaml
services:
  s3:
    image: ghcr.io/wow-look-at-my/go-s3-server
    command: ["--config", "/data/config.json"]
    volumes: ["/data:/data"]
    stop_grace_period: 300s   # let the drain finish before Docker sends SIGKILL
    labels:
      docker-updater.enable: "true"
      docker-updater.rolling: "true"
      docker-updater.health-check.url: ":9000/_health"  # ":port" → container IP
```

docker-updater resolves the `:`-prefixed URL to the new container's IP and polls
it until it returns `2xx`. In recreate mode (omit `docker-updater.rolling`) the
same `/_health` works as the post-update health check; the optional
`docker-updater.pre-check.url` gate is consulted only in recreate mode, not
rolling. For a shared build cache, prefer **rolling** mode — it keeps the cache
reachable throughout the deploy while the old instance drains.

## Docker

```
docker run -v /data:/data ghcr.io/wow-look-at-my/go-s3-server --config /data/config.json
```

## Building

This project uses [go-toolchain](https://github.com/wow-look-at-my/go-toolchain). Run from the project root:

```
go-toolchain
```
