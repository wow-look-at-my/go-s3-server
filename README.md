# go-s3-server

Shared build cache server for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain), backed by the local filesystem. (Slated to be renamed **go-toolchain-cache**.)

This server speaks go-toolchain's native cache protocol. It began life S3-compatible, but S3 is not an efficient format for exchanging build-cache objects. So the protocol was replaced with a faster, purpose-built design — a binary key index (`/_index`, GBCI v1) and a batched tar transfer (`/_batch/get`) with prefetch. The only S3 surface that remains is **deprecated, not removed**. The legacy `X-Amz-Meta-*` request headers are still accepted, so not-yet-upgraded clients keep working. Every use is logged once and counted in `s3_deprecated_requests_total`. The shim will be dropped when the repository is renamed. See [Cache protocol & deprecations](#cache-protocol--deprecations).

## Features

- **Object API** — `GET`/`HEAD`/`PUT`/`DELETE` of cache objects by key (`DELETE` is idempotent, returns `204`. The surgical lever for evicting a single poisoned cache entry without a whole-cache version-bump purge). `HEAD` is the cheap inspection endpoint: the exact `GET` header surface (metadata, `Content-Length`, `Last-Modified`) with no body and no cache-state side effects. Errors are native plain text (`<code>: <message>`, with the code repeated in an `X-Cache-Error-Code` header) — not S3 XML.
- **Cache-key index** — `GET /<bucket>/_index` returns a precomputed binary blob (GBCI v1) of every cacheprog action-ID hash, with strong ETag and `If-None-Match` 304 support. The blob (and thus the ETag) is a pure function of the advertised key set, so duplicate-only PUT traffic and server restarts never invalidate clients' cached copies.
- **Self-healing reads (in-place repair)** — an indexed cacheprog object with no `outputid` can never be a hit, yet stays advertised in `_index`: a permanent forced miss. The server **repairs it in place** on read, recomputing the `outputid` from the body. Depth: [docs/object-handlers.md](docs/object-handlers.md). Counted by `s3_self_heal_repairs_total`.
- **Module-index rejection (write *and* read)** — a mis-keyed Go module index breaks every consumer's build at package load, and no client-side check catches it. The server refuses to store one, and evicts one already on disk when it is read. Depth: [docs/module-index-guard.md](docs/module-index-guard.md).
- **HTTP Basic Auth** — multiple users, or explicitly disable with `disable_auth: true`
- **Write-once mode** — deny overwriting existing keys with configurable conflict notification (ideal for content-addressable caches)
- **Bounded cache (automatic eviction)** — a background sweeper prunes by idle age (`max_age`) and total size (`max_bytes`). The `data_dir` therefore cannot fill the disk. Eviction goes by *last use*, not write time. See [Cache eviction](#cache-eviction).
- **Sharded storage** — keys are automatically split into a two-level directory tree to avoid huge flat directories
- **Streaming, OOM-safe under load** — bodies stream straight to and from disk, so no whole object is ever buffered. A concurrency limit sheds excess load with `503 + Retry-After` rather than queueing until it OOMs. See [Behavior under load](#behavior-under-load).
- **Graceful drain on shutdown** — on `SIGTERM`/`SIGINT` the server stops taking new requests and lets in-flight ones finish. A rolling update therefore cuts off no CI transfer. An unauthenticated `GET /_health` returns `200` when ready and `503` while draining. See [Graceful shutdown & rolling updates](#graceful-shutdown--rolling-updates).
- **Multi-arch Docker image** — `linux/amd64` and `linux/arm64` published to `ghcr.io/wow-look-at-my/go-s3-server`

## Cache protocol & deprecations

The cache protocol is **no longer S3-compatible**. Clients (go-toolchain) talk to it with:

- **Object transfer** — `GET`/`PUT`/`DELETE /<bucket>/<key>`. Object metadata travels in native `X-Cache-Meta-*` headers (e.g. `X-Cache-Meta-Outputid`). Errors are native plain text with an `X-Cache-Error-Code` header.
- **Key index** — `GET /<bucket>/_index` returns the GBCI v1 binary blob (the client loads it once to know which keys exist, instead of probing per key).
- **Batched fetch** — `POST /<bucket>/_batch/get` (JSON body of keys.`GET` with a body is also still accepted for older clients) returns a tar of bodies + a `manifest.json`, with temporal prefetch of related entries. This is the scalable replacement for per-object S3 GETs.
- **Batched upload** — `PUT /<bucket>/_batch/put` (Content-Type `application/x-tar`) stores many objects in a single request. The tar holds a `manifest.json` first member (`{"entries":[{"key":...,"metadata":{...}}]}`, metadata keyed by the lowercased meta name without the `X-Cache-Meta-` prefix) followed by one `data/<key>` member per entry in manifest order. The response is JSON `{"results":[{"key":...,"status":"stored|dropped|conflict|error","message":...}]}`. Each member is stored through the same path as a single `PUT` (module-index refusal — counted in `s3_put_refusals_total` — write_once, audit xattrs, index append). The whole batch holds **one** admission-control slot. It replaces the thousands of per-object `PUT`s a CI build otherwise issues, each taking a slot and saturating the server. A batch past the entry cap is refused with `413`. A malformed tar, missing/late `manifest.json`, or a key mismatch between the manifest and the data members is a `400 invalid_request`.
- **Object inspection** — `HEAD /<bucket>/<key>` answers with the object's metadata headers and size, no body.

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
  "eviction": {"max_bytes": 53687091200, "interval": "24h"},
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
| `--metrics-listen` | Address for the Prometheus `/metrics` server (e.g. `:9090`) |
| `--dashboard-listen` | Address for the operator dashboard (default `:9002`). `off` disables it. |
| `--log-mode` | Access log shape: `normal` (default) or `verbose` |

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
| `eviction` | object | `{"max_bytes":53687091200,"interval":"24h"}` | no | Automatic pruning of the cache (see below). |
| `log_mode` | string | `normal` | no | Access log shape. `normal` prints one aggregated line per active second; `verbose` prints one line per request. See [Logging](#logging). |
| `dashboard_listen` | string | `:9002` | no | Operator dashboard, on its own port. `""` disables it. It answers without credentials, so front it with an access proxy — see [Dashboard](#dashboard). |

### Environment variables

| Variable | Description |
|----------|-------------|
| `CACHE_MAX_BYTES` | Cache size budget when `eviction.max_bytes` is not set in the config: a byte count or a size like `100GB`. A malformed value fails startup rather than falling back. |
| `GOMEMLIMIT` | Standard Go setting. Left alone if set; otherwise go-toolchain's injected startup guard installs 90% of the container's memory limit. The server reads whichever ceiling is in effect and sizes its in-memory caches against it. |

### `write_once` options

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `action` | `allow`, `deny` | `allow` | Whether to allow overwriting existing keys |
| `notification` | `never`, `always`, `content_differs` | `never` | When to return HTTP 409 on overwrite attempts |

- `action: "deny"` + `notification: "never"` — silently skip overwrites (200 response)
- `action: "deny"` + `notification: "always"` — reject any overwrite attempt (409 response)
- `action: "deny"` + `notification: "content_differs"` — reject only when content differs. Same content is idempotent (ideal for content-addressable caches)

### `eviction` options

The cache is an **LRU**: it is bounded by size, and over budget the least recently used entries go first. Age eviction is a separate opt-in TTL.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_bytes` | int | `53687091200` (50 GiB), or `CACHE_MAX_BYTES` | Total-size budget for `data_dir` in bytes. Over budget, least-recently-used entries are evicted until the total is back under it. `0` disables size-based eviction. |
| `max_age` | duration | `"0"` (off) | Also remove entries not *used* within this window, however small the cache is. A Go duration string (`"720h"`, `"30m"`) or seconds. |
| `interval` | duration | `"24h"` | How often the background sweeper runs. The last sweep is recorded in `data_dir`, so a restart does not reset the schedule: the server sweeps at startup when the last one is at least an interval old, and otherwise waits out the remainder. |

Setting both `max_bytes: 0` and `max_age: "0"` disables eviction entirely (the server logs a warning that the cache will grow without bound).

"Last used" is the latest of an entry's write time, its filesystem access time, and any read this process saw. Access times survive restarts, so an old entry that is still being read is never mistaken for an idle one. A `noatime` mount carries no such signal. The server says so at startup and tracks reads in memory instead.

## Logging

`log_mode` (or `--log-mode`) picks the access log's shape. The default, `normal`, prints one line per second in which the cache moved an object, and nothing for a silent second:

```
cache 1s: put=0 get=58 batched=72% compressed=3.4KiB/s uncompressed=13KiB/s ratio=26% projects=github.com/wow-look-at-my/go-s3-server
```

Those fields are objects stored, objects served, and the share of them that went through the batch endpoints. Then the byte rates on and off the wire, the resulting compression ratio, and the modules the traffic belonged to.

`verbose` prints one line per request instead, with any handler detail (a batch's key counts, for example) on that same line. It is for following one client. Under CI load the per-request lines bury what you are looking for. Depth: [docs/logging.md](docs/logging.md).

## Dashboard

An operator dashboard runs on its **own port** (`dashboard_listen`, default `:9002`), separate from the cache protocol port. It shows hit rate, cache size against budget, batch volume, the tripwire counters, memory, and the running config, polled every 5s.

It answers **without credentials, by design**: publish that port through an access proxy such as Cloudflare Zero Trust, and leave the cache port alone (build machines carry basic-auth credentials, not browser sessions). Set `dashboard_listen` to `""`, or pass `--dashboard-listen off`, to run without it.

Numbers come from the same Prometheus registry `/metrics` serves. The page cannot disagree with a scrape. The UI is built on the org design language, [scratch_ui](https://github.com/wow-look-at-my/scratch_ui), loaded at runtime — so a browser viewing the dashboard needs to reach `sites.pazer.build`.

Full setup, including the two-hostname tunnel config: [docs/dashboard.md](docs/dashboard.md).

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

Every `PutObject` request records the following fields as extended attributes on the stored file (namespace `user.s3audit.*` on Unix, `.audit` sidecar on Windows):

| Attribute | Source |
|-----------|--------|
| `uploader` | Authenticated username (or `<ANON>` when `disable_auth: true`) |
| `uploaded_at` | Server wall clock at request start (RFC 3339 nano) |
| `client_ip` | `CF-Connecting-IP` → `X-Real-IP` → first `X-Forwarded-For` → TCP peer |
| `user_agent` | Request `User-Agent` header |
| `content_length` | Size in bytes of the uploaded body |

Inspect on Linux with `getfattr -d path/to/object`. These fields exist so a suspected compromise can be investigated without guesswork.

The same fields are written to the server's request log on every request.

## Cache version

The server stamps each `data_dir` with a cache version marker (`.cache_version`). On startup, a stored version that does not match the server's current version wipes the entire `data_dir` before any request is served. This is a one-way safety net: when the maintainers ship a fix that must invalidate previously-stored content (for example, after closing a vulnerability that can let an attacker populate the cache), they bump the version. Next restart on every deployment forces the cache to be rebuilt from trusted inputs.

A corrupt or unparseable marker file is a startup error — fix it manually, do not leave it to a silent purge.

## Cache eviction

A build cache accumulates entries forever as code changes. Every new action ID is a new object, and old ones are never referenced again. Without pruning, the `data_dir` grows until the disk fills. A background sweeper prevents that, with two independent and combinable limits (configured under [`eviction`](#eviction-options)):

- **`max_age`** — remove entries not used within the window (default 30 days).
- **`max_bytes`** — keep the total cache size under a budget, evicting least-recently-used entries first when it is exceeded.

"Used" means the later of an entry's write time (mtime) and its last read. Last-read time is tracked in memory while the server runs. A frequently fetched but rarely rewritten object therefore stays alive, which is what a content-addressed cache wants. Read time is *not* written back to the file's mtime. mtime stays the entry's write time, which the prefetch system groups "same build" entries by. Across a restart the in-memory read times reset, so entries age from their mtime until read again. At worst that delays an eviction, and it never hastens one wrongly.

Eviction never threatens correctness: a wrongly evicted entry is simply a cache miss that the next build recomputes and re-uploads. Evicted counts and reclaimed bytes are exported as `s3_evictions_total`, `s3_evicted_bytes_total`, and `s3_cache_bytes` (see below).

The sweeper runs its **first sweep a jittered 1-5 minutes after startup**, then every `interval`. (Waiting a full interval for the first sweep meant a deployment that restarts more often than the interval — rolling updates — never evicted at all.) Between sweeps the `s3_cache_bytes` gauge is refreshed every 15 minutes from a size-only walk, so growth is visible without waiting for a sweep. Before deleting a victim the sweeper re-checks its on-disk mtime, and skips anything overwritten since the scan. Every victim is dropped from `/_index` *before* its file is unlinked. A mid-sweep fetch therefore sees a re-uploadable miss, never a 404 on an advertised key. Leftover `.tmp-*` files from interrupted uploads are swept once at startup.

Eviction is **on by default** with a conservative 30-day idle window. To opt out entirely, set both limits off:

```json
"eviction": {"max_age": "0", "max_bytes": 0}
```

The server then logs a startup warning that the cache will grow without bound.

## Behavior under load

This server is built to absorb the concurrent load of a parallel CI matrix (many jobs each batch-fetching and uploading hundreds of content-addressed keys) without OOM-ing or returning `502`s:

- **Bodies are streamed, never buffered.** Every read and write path copies object bytes directly between disk and the socket, through a fixed-size buffer. A batch holds one body in flight at a time. Memory therefore stays flat however many objects a client asks for at once.
- **Backpressure, not collapse.** At most `max_concurrent_requests` are served at once. Further requests are shed immediately with `503 Service Unavailable` and a `Retry-After` header — a signal clients back off on. The server never queues unbounded work until the process is OOM-killed (the failure a fronting proxy reports as a `502`). Overload-shed requests are counted in the `s3_http_rejected_total` metric.
- **Memory-bounded caches, never refused requests.** The server reads its own memory ceiling (`GOMEMLIMIT`, or the container's cgroup limit) and sizes its in-memory caches against it. As memory fills, those caches shrink and evict. Requests are never shed for memory — a cache that refuses to serve is worse than no cache. See [docs/memory-limits.md](docs/memory-limits.md).
- **Bounded requests.** A single PUT is capped at `max_object_bytes` (`413` over the limit). A `_batch/get` is capped at `maxBatchKeys` keys (`400` over it). A `_batch/put` is capped at that same key count and at `maxBatchKeys × max_object_bytes` of body (`413` over either), with each member bounded to `max_object_bytes`.
- **Timeouts.** The HTTP server sets `ReadHeaderTimeout` (slowloris guard) plus generous `Read`/`Write`/`Idle` timeouts so a stuck connection cannot pin a concurrency slot indefinitely.
- **Warm-key fast path.** The read-path module-index probe costs a file open plus an lz4 first-block decode, and runs once per key. A sharded in-memory memo remembers keys whose body already passed it, and is invalidated on overwrite, delete and eviction. Steady-state GETs of warm keys skip the decode.
- **Observability.** When `--metrics-listen` is set, `/metrics` exposes request, storage, in-flight, and rejection counters, plus:
  - `s3_get_requests_total{outcome}` — every single-object GET by outcome: `hit`, `miss_not_found`, `miss_advertised_unservable` (a 404 on a key `/_index` currently advertises — the index/store-divergence signature that must stay at ~0), `miss_module_index_evicted`, `miss_peek_error`, `miss_selfheal_failed`.
  - `s3_put_refusals_total{reason}` — uploads accepted on the wire but refused storage (e.g. `module_index`). This moving during CI activity is the PUT guard's liveness proof.
  - `s3_batch_requests_total` and `s3_batch_keys_total{kind}` (`requested`/`found`/`prefetched`/`suppressed`/`streamed`) — batch volume. A falling found/requested ratio is the earliest cache-degradation signal.
  - index gauges `s3_index_entries`, `s3_index_hashes`, `s3_index_pending_hashes` and `s3_index_rebuild_duration_seconds`.
  - eviction counters (`s3_evictions_total`, `s3_evicted_bytes_total`) and the cache size `s3_cache_bytes` (refreshed every 15 minutes, not just at sweep end).
  - self-heal counters: `s3_self_heal_repairs_total` (outputid-less relics repaired in place on read), `s3_self_heal_failures_total` (unrepairable bodies, de-advertised so consumers re-upload), and `s3_outputid_mismatch_total` (a stored outputid found disagreeing with its body hash — stale-stamp corruption, repaired in place).
  - `s3_module_index_evictions_total` (module-index blobs refused + evicted on a read path) and `s3_metadata_xattrs_dropped_total` (optional metadata dropped under xattr-space pressure instead of failing the PUT).
  - `s3_meta_cache_hits_total` / `s3_meta_cache_misses_total` — object metadata served from memory vs read back from extended attributes. On a warm cache this ratio is the read path's CPU story.
  - memory: `s3_memory_limit_bytes` (the discovered ceiling, 0 = none found), `s3_memory_in_use_bytes`, `s3_memory_shrinks_total` (times cache budgets were cut under pressure) and `s3_cache_memory_bytes{cache}` / `s3_cache_memory_budget_bytes{cache}` (what each cache holds and is allowed to hold). There is no "refused for memory" metric because nothing is.

All alongside the standard Go runtime and process collectors (`go_memstats_*`, `process_resident_memory_bytes`, `go_goroutines`) — enough to see saturation, memory pressure, cache growth, and cache health directly. A busy metrics port no longer prevents the cache from starting. The server logs the failure and runs without metrics.

## Graceful shutdown & rolling updates

When told to stop, the server drains in-flight requests instead of dropping them. A rolling deploy, or any `docker stop`, therefore cuts off no CI upload or batch download.

- **`GET /_health`** — an unauthenticated readiness probe. Returns `200` with body `ok` while serving, and `503` (with `Retry-After`) once a shutdown has begun. It is answered *before* authentication and admission control. So it needs no S3 credentials and is never shed under load — point your reverse proxy and orchestrator at it.
- **Drain on signal.** On `SIGTERM`/`SIGINT` the server marks `/_health` unhealthy (`503`) and calls `http.Server.Shutdown`. The listener closes immediately, so new connections are refused. In-flight requests get a 280s drain budget to finish, which is kept under a typical container stop grace period.
- **Where the `503` actually reroutes traffic.** The `503` tells a **health-checking** proxy or load balancer in front to take this instance out of rotation. New requests then go to the replacement. It is *not* automatic. docker-updater does not poll the old container's `/_health` while draining, and Docker's DNS round-robin is not health-aware. With no health-aware front end the `503` is informational — rerouting then comes only from the closed listener refusing new connections, not from the probe.

### Rolling update with docker-updater

[`docker-updater`](https://github.com/wow-look-at-my/docker-updater) performs a zero-downtime update against this drain. It starts the replacement, waits for the new instance's `/_health` to go green, then stops the old one. The stop grace period is long enough for the old instance to drain. Label the container:

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
      docker-updater.well-known.port: "9000"   # which port serves the endpoints
```

The server answers `GET /.well-known/docker-updater/health` and `/.well-known/docker-updater/pre-update` — the contract docker-updater discovers by itself, no check labels required. Both are aliases of the `/_health` logic: `200` normally, `503` while draining. Discovery still needs to know which port to probe, and this image declares none, so `docker-updater.well-known.port` names it. Everything else is automatic.

The older `docker-updater.health-check.url: ":9000/_health"` form still works and still wins where it is set (the `:`-prefixed URL resolves to the container's own IP), but it marks the container "nonstandard" on the dashboard. In recreate mode (omit `docker-updater.rolling`) the pre-update gate is consulted. Rolling mode skips it. For a shared build cache, prefer **rolling** mode — it keeps the cache reachable throughout the deploy while the old instance drains.

## Docker

```
docker run -v /data:/data ghcr.io/wow-look-at-my/go-s3-server --config /data/config.json
```

## Building

This project uses [go-toolchain](https://github.com/wow-look-at-my/go-toolchain). Run from the project root:

```
go-toolchain
```
