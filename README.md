# go-s3-server

Shared build cache server for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain), backed by the local filesystem. (Slated to be renamed **go-toolchain-cache**.)

This server speaks go-toolchain's native cache protocol. It began life S3-compatible, but S3 is not an efficient format for exchanging build-cache objects, so the protocol was replaced with a faster, purpose-built design — a binary key index (`/_index`, GBCI v1) and a batched tar transfer (`/_batch/get`) with prefetch. The only S3 surface that remains is **deprecated, not removed**: the legacy `X-Amz-Meta-*` request headers are still accepted so not-yet-upgraded clients keep working, but every use is logged once and counted in `s3_deprecated_requests_total`, and the shim will be dropped when the repository is renamed. See [Cache protocol & deprecations](#cache-protocol--deprecations).

## Features

- **Object API** — `GET`/`PUT`/`DELETE` of cache objects by key (`DELETE` is idempotent, returns `204`; the surgical lever for evicting a single poisoned cache entry without a whole-cache version-bump purge). Errors are native plain text (`<code>: <message>`, with the code repeated in an `X-Cache-Error-Code` header) — not S3 XML.
- **Cache-key index** — `GET /<bucket>/_index` returns a precomputed binary blob (GBCI v1) of every cacheprog action-ID hash, with strong ETag and `If-None-Match` 304 support
- **Self-healing reads (in-place repair)** — an indexed cacheprog object (`go-buildcache/v1<hash>`) with no `outputid` metadata can never be a cache hit (the client needs the content address to verify the body) yet pins its key in `_index`, so clients skip re-uploading it — a permanent forced miss. Such relics (from earlier cache-data iterations, or an xattr-stripping `data_dir` move) are **repaired in place** the moment they are read: the `outputid` is, by definition, `sha256` of the decompressed body, so the server recomputes it from the body and writes it back as an xattr. The object keeps its bytes and its audit trail, stays in `_index`, and serves as a hit — no eviction, no re-upload, no churn, and the repair is one-time. A body that can't be decompressed (and so is unusable by the client anyway) is reported as a clean miss and left untouched for the normal eviction policy. Counted by `s3_self_heal_repairs_total`.
- **HTTP Basic Auth** — multiple users, or explicitly disable with `disable_auth: true`
- **Write-once mode** — deny overwriting existing keys with configurable conflict notification (ideal for content-addressable caches)
- **Bounded cache (automatic eviction)** — a background sweeper prunes entries by idle age (`max_age`) and/or a total-size budget (`max_bytes`) so the `data_dir` never grows until the disk fills. Eviction is by *last use* (read or write), not just write time. On by default with a conservative 30-day idle window; see [Cache eviction](#cache-eviction).
- **Sharded storage** — keys are automatically split into a two-level directory tree to avoid huge flat directories
- **Streaming, OOM-safe under load** — object bodies are streamed straight to/from disk on GET, PUT, and batch GET, so the server never buffers whole objects in memory. A concurrency limit sheds excess load with `503 + Retry-After` instead of queueing until it OOMs (which a fronting proxy would surface as a `502`). See [Behavior under load](#behavior-under-load).
- **Graceful drain on shutdown** — on `SIGTERM`/`SIGINT` the server stops accepting new requests and lets in-flight ones finish (up to a drain timeout) before exiting, so a rolling update never cuts off an in-progress CI upload or download. An unauthenticated `GET /_health` probe returns `200` when ready and `503` while draining. See [Graceful shutdown & rolling updates](#graceful-shutdown--rolling-updates).
- **Multi-arch Docker image** — `linux/amd64` and `linux/arm64` published to `ghcr.io/wow-look-at-my/go-s3-server`

## Cache protocol & deprecations

The cache protocol is **no longer S3-compatible**. Clients (go-toolchain) talk to it with:

- **Object transfer** — `GET`/`PUT`/`DELETE /<bucket>/<key>`. Object metadata travels in native `X-Cache-Meta-*` headers (e.g. `X-Cache-Meta-Outputid`). Errors are native plain text with an `X-Cache-Error-Code` header.
- **Key index** — `GET /<bucket>/_index` returns the GBCI v1 binary blob (the client loads it once to know which keys exist, instead of probing per key).
- **Batched fetch** — `GET /<bucket>/_batch/get` (JSON body of keys) returns a tar of bodies + a `manifest.json`, with temporal prefetch of related entries. This is the scalable replacement for per-object S3 GETs.

### Deprecated (still works, warns on use)

| Feature | Replacement | Behavior |
|---------|-------------|----------|
| `X-Amz-Meta-*` request headers | `X-Cache-Meta-*` | Still accepted on `PUT`; still emitted on `GET` alongside the native header so old clients keep hitting the cache. First use logs a `DEPRECATION:` warning; every use increments `s3_deprecated_requests_total{feature="amz_meta_header"}`. |

These shims exist so a fleet of pinned/older go-toolchain clients keeps working during the rollout. Once `s3_deprecated_requests_total` stays flat at zero, the shims (and the `s3_`/`bucket` naming) are removed as part of renaming this repository to **go-toolchain-cache**. Nothing here changes on-disk storage, so no cache rebuild is required to deploy this server.

## Quick start

Create a JSON config file:

```json
{
  "listen": ":9000",
  "bucket": "my-cache",
  "data_dir": "/var/data/s3",
  "write_once": {"action": "deny", "notification": "content_differs"},
  "eviction": {"max_age": "720h", "max_bytes": 53687091200, "interval": "72h"},
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
| `bucket` | string | — | yes | Cache namespace served at `/<bucket>/...` (kept as `bucket` until the repo rename) |
| `data_dir` | string | — | yes | Directory to store objects |
| `write_once` | object | `{"action":"allow"}` | no | Write-once behavior (see below) |
| `disable_auth` | bool | `false` | no | If `true`, accept all requests without authentication. Must be set explicitly; `credentials` must be omitted when this is `true`. |
| `credentials` | array | — | yes (unless `disable_auth: true`) | One or more `username`/`password` pairs. Both fields must be non-empty. |
| `max_concurrent_requests` | int | `128` | no | Max in-flight requests; excess is shed with `503 + Retry-After`. `0` → default. |
| `max_object_bytes` | int | `1073741824` (1 GiB) | no | Max single PUT body; larger uploads get `413`. The body is streamed to disk, so this guards disk, not memory. `0` → default. |
| `eviction` | object | `{"max_age":"720h"}` | no | Automatic pruning of the cache (see below). |

### `write_once` options

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `action` | `allow`, `deny` | `allow` | Whether to allow overwriting existing keys |
| `notification` | `never`, `always`, `content_differs` | `never` | When to return HTTP 409 on overwrite attempts |

- `action: "deny"` + `notification: "never"` — silently skip overwrites (200 response)
- `action: "deny"` + `notification: "always"` — reject any overwrite attempt (409 response)
- `action: "deny"` + `notification: "content_differs"` — reject only when content differs; same content is idempotent (ideal for content-addressable caches)

### `eviction` options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_age` | duration | `"720h"` (30 days) | Remove entries not *used* within this window. A Go duration string (`"720h"`, `"30m"`) or a number of seconds. `"0"` disables age-based eviction. |
| `max_bytes` | int | `0` (disabled) | Total-size budget for `data_dir` in bytes. When exceeded, least-recently-used entries are evicted until the total is back under budget. `0` disables size-based eviction. |
| `interval` | duration | `"72h"` (3 days) | How often the background sweeper runs. With a 30-day `max_age` there is little point sweeping more often; shorten it only if you set a tight `max_bytes` and want to bound how far the cache overshoots the budget between sweeps. |

Setting both `max_age: "0"` and `max_bytes: 0` disables eviction entirely (the server logs a warning that the cache will grow without bound).

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

## Cache eviction

A build cache accumulates entries forever as code and dependencies change —
every new action ID is a new object, and old ones are never referenced again.
Without pruning, the `data_dir` grows until the disk fills. A background sweeper
prevents that, with two independent and combinable limits (configured under
[`eviction`](#eviction-options)):

- **`max_age`** — remove entries not used within the window (default 30 days).
- **`max_bytes`** — keep the total cache size under a budget, evicting
  least-recently-used entries first when it is exceeded.

"Used" means the later of an entry's write time (mtime) and its last read.
Last-read time is tracked in memory while the server runs, so a frequently
fetched but rarely rewritten object is kept alive — exactly what you want for a
content-addressed cache where popular entries are written once and read forever.
(Read time is *not* written back to the file's mtime: mtime stays the entry's
write time, which the prefetch system relies on to group "same build" entries.)
Across a restart the in-memory read times reset, so entries are aged from their
mtime until they are read again — at worst this delays, never wrongly hastens,
an eviction.

Eviction never threatens correctness: a wrongly evicted entry is simply a cache
miss that the next build recomputes and re-uploads. Evicted counts and reclaimed
bytes are exported as `s3_evictions_total`, `s3_evicted_bytes_total`, and
`s3_cache_bytes` (see below).

Eviction is **on by default** with a conservative 30-day idle window. To opt out
entirely, set both limits off:

```json
"eviction": {"max_age": "0", "max_bytes": 0}
```

The server then logs a startup warning that the cache will grow without bound.

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
  storage, in-flight, and rejection counters, plus eviction counters
  (`s3_evictions_total`, `s3_evicted_bytes_total`) and `s3_self_heal_repairs_total`
  (outputid-less relics repaired in place on read) and the current cache size
  (`s3_cache_bytes`), alongside the standard Go runtime and process collectors
  (`go_memstats_*`, `process_resident_memory_bytes`, `go_goroutines`) — enough to
  see saturation, memory pressure, and cache growth directly.

## Graceful shutdown & rolling updates

The server drains in-flight requests instead of dropping them when told to stop,
so a rolling deploy (or any `docker stop`) does not cut off an in-progress CI
upload or batch download.

- **`GET /_health`** — an unauthenticated readiness probe. Returns `200` with
  body `ok` while serving, and `503` (with `Retry-After`) once a shutdown has
  begun. It is answered *before* authentication and admission control, so it
  needs no S3 credentials and is never shed under load — point your reverse
  proxy and orchestrator at it.
- **Drain on signal.** On `SIGTERM`/`SIGINT` the server marks `/_health`
  unhealthy (`503`) and calls `http.Server.Shutdown`: the listener closes
  immediately (new connections are refused) while in-flight requests are given
  up to a 280s drain budget — kept under a typical container stop grace period —
  to finish before the process exits.
- **Where the `503` actually reroutes traffic.** The `503` is the signal for a
  **health-checking** reverse proxy or load balancer in front (Traefik / nginx /
  HAProxy with active health checks, a Cloudflare load balancer, a k8s readiness
  probe, etc.) to take this instance out of rotation so new requests go to the
  replacement. It is *not* automatic: docker-updater does not poll the old
  container's `/_health` while draining, and Docker's built-in DNS round-robin is
  not health-aware. With no health-aware front end the `503` is informational —
  rerouting then comes only from the closed listener refusing new connections,
  not from the probe.

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
