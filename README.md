# go-s3-server

Minimal S3-compatible server backed by the local filesystem. Designed as a shared build cache for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain).

## Features

- **S3 API subset** — `GetObject`, `PutObject`, `DeleteObject` (idempotent, returns `204`; the surgical lever for evicting a single poisoned cache entry without a whole-cache version-bump purge)
- **Cache-key index** — `GET /<bucket>/_index` returns a precomputed binary blob (GBCI v1) of every cacheprog action-ID hash, with strong ETag and `If-None-Match` 304 support
- **HTTP Basic Auth** — multiple users, or explicitly disable with `disable_auth: true`
- **Write-once mode** — deny overwriting existing keys with configurable conflict notification (ideal for content-addressable caches)
- **Bounded cache (automatic eviction)** — a background sweeper prunes entries by idle age (`max_age`) and/or a total-size budget (`max_bytes`) so the `data_dir` never grows until the disk fills. Eviction is by *last use* (read or write), not just write time. On by default with a conservative 30-day idle window; see [Cache eviction](#cache-eviction).
- **Sharded storage** — keys are automatically split into a two-level directory tree to avoid huge flat directories
- **Streaming, OOM-safe under load** — object bodies are streamed straight to/from disk on GET, PUT, and batch GET, so the server never buffers whole objects in memory. A concurrency limit sheds excess load with `503 + Retry-After` instead of queueing until it OOMs (which a fronting proxy would surface as a `502`). See [Behavior under load](#behavior-under-load).
- **Multi-arch Docker image** — `linux/amd64` and `linux/arm64` published to `ghcr.io/wow-look-at-my/go-s3-server`

## Quick start

Create a JSON config file:

```json
{
  "listen": ":9000",
  "bucket": "my-cache",
  "data_dir": "/var/data/s3",
  "write_once": {"action": "deny", "notification": "content_differs"},
  "eviction": {"max_age": "720h", "max_bytes": 53687091200, "interval": "1h"},
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
| `interval` | duration | `"1h"` | How often the background sweeper runs. |

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
miss that the next build recomputes and re-uploads. The sweeper runs every
`interval` (default 1h); the first sweep is one interval after startup. Evicted
counts and reclaimed bytes are exported as `s3_evictions_total`,
`s3_evicted_bytes_total`, and `s3_cache_bytes` (see below).

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
  (`s3_evictions_total`, `s3_evicted_bytes_total`) and the current cache size
  (`s3_cache_bytes`), alongside the standard Go runtime and process collectors
  (`go_memstats_*`, `process_resident_memory_bytes`, `go_goroutines`) — enough to
  see saturation, memory pressure, and cache growth directly.

## Docker

```
docker run -v /data:/data ghcr.io/wow-look-at-my/go-s3-server --config /data/config.json
```

## Building

This project uses [go-toolchain](https://github.com/wow-look-at-my/go-toolchain). Run from the project root:

```
go-toolchain
```
