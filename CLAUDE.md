# go-s3-server

Minimal S3-compatible server for use as a shared Go build cache.

## Build & test

```
go-toolchain
```

Do NOT use `go build`, `go test`, or any bare `go` commands. Always use `go-toolchain` from the project root.

## Project layout

- `main.go` — CLI entry point (cobra). Serves in a goroutine and waits for `SIGINT`/`SIGTERM`, then calls `srv.BeginShutdown()` (flips `/_health` to 503) and `httpSrv.Shutdown(ctx)` with a `shutdownTimeout` (280s, kept under a typical container stop grace) so in-flight requests drain before exit instead of being cut off by a rolling-update `docker stop`
- `config.go` — JSON config loading and validation
- `auth.go` — HTTP Basic Auth; returns the authenticated username
- `auth_test.go` — Auth tests (including the "empty credential must not bypass auth" regression)
- `server.go` — HTTP router, auth gate, client-IP resolution, audit context, bucket dispatch. **Admission control**: a buffered `sem` channel sized to `max_concurrent_requests` bounds in-flight requests; when full, excess requests are shed with `503 + Retry-After` (never queued to OOM, which a proxy would report as `502`). `NewServer` defaults the limit so a directly-constructed `Config` is safe too. **Health/readiness**: an unauthenticated `GET /_health` probe is handled at the very top of `ServeHTTP` — before logging, metrics, auth, and admission control — returning `200`/`ok` normally and `503` once `BeginShutdown` sets the `shuttingDown` flag (so a health-checking proxy/LB in front, if any, takes the instance out of rotation during graceful shutdown — the drain itself relies on `Shutdown` closing the listener, not on the probe being watched).
- `server_test.go` — Health/readiness endpoint tests (`200` ok, `503` while draining, bypasses both the auth gate and admission control)
- `handlers.go` — S3 API handlers (GetObject, PutObject, DeleteObject) plus `handleGetIndex` for the `/_index` endpoint. GetObject and PutObject **stream** bodies (no whole-object buffering): GET `io.Copy`s from `storage.Open`; PUT streams via `storage.PutStream` wrapped in `http.MaxBytesReader(max_object_bytes)` (413 over the limit). PutObject writes audit xattrs (`content_length` is filled in by PutStream from the bytes actually streamed). DeleteObject is idempotent (204 even for a missing key) and is the surgical eviction lever for a poisoned cache entry — it removes the file and drops the key from the index (`Index.Remove`)
- `index.go` — In-memory key index. Maintains an mtime-sorted list for `_batch/get` prefetch *and* a sorted slice of action-ID hashes that serializes to the GBCI v1 binary blob served at `GET /<bucket>/_index`. PUTs append to unsorted `pending` (hashes) and `pendingEntries` (mtime) buffers under a microsecond-scale mutex; both the hash sort+dedupe+serialize (deferred to the next `Blob()`) and the mtime merge+sort (deferred to the next `NearbyKeys`/`Remove` via `drainEntriesLocked`) are kept off the per-PUT path, so PUT is an O(1) append and writers don't convoy on a global sort.
- `storage.go` — Filesystem storage with two-level key sharding, cache version auto-purge, and single-key `Delete` (used by DeleteObject). Bodies are streamed, never buffered: `PutStream` (io.Copy to a temp file, then write_once via streaming `filesEqual`), `Open` (returns an `*os.File` + meta for streaming reads), and `Stat` (meta/size only, no body) back the GET and batch paths. `Put([]byte,...)` is a thin wrapper over `PutStream` for in-memory callers and tests. Also holds **last-access tracking** for eviction: a sharded in-memory map (`accessShards`, 256 shards keyed by FNV-1a) updated by `recordAccess` on every `Open`/`Get`. Allocated only when `EnableAccessTracking` is called (eviction enabled), so the read hot path is free when eviction is off. Access time is kept *separate* from mtime — mtime stays the write time the prefetch system depends on.
- `eviction.go` — Background cache pruning. `Evict(maxAge, maxBytes, now)` runs one sweep: an **age pass** removes entries idle longer than `max_age` (idle = `now - max(mtime, lastAccess)`), then a **size pass** evicts least-recently-used survivors until total size is under `max_bytes`. Deletes files directly and rebuilds the index once at the end (not O(n) `Index.Remove` per victim → avoids O(n²)). `RunEvictionLoop` drives it on a ticker. Wired up in `main.go` only when `Eviction.Enabled()`; otherwise main logs a loud unbounded-growth warning.
- `eviction_test.go` — Eviction tests (age, access-time survival, size/LRU, index rebuild, access-record cleanup, Duration/config parsing)
- `storage_test.go` — Cache version / purge tests
- `storage_unix.go` / `storage_windows.go` — Platform-specific file locking, user metadata xattrs, and server audit xattrs
- `lock_windows.go` — Windows file locking via syscall
- `handlers_test.go` — Unit tests for handlers
- `.github/workflows/ci.yml` — CI pipeline (build, docker, s3-api-test, integration test)
- `.github/workflows/integration-test/` — Integration test harness (configs, test Go project)

## Conventions

- CLI parsing uses cobra. The single root command is in `main.go`.
- S3 error responses are XML-encoded using `encoding/xml`. The `/_index` endpoint serves a binary GBCI v1 blob (`application/octet-stream`) with a strong ETag (hex-encoded SHA-256 trailer); conditional GETs are handled by `http.ServeContent`.
- Object metadata is stored in filesystem extended attributes (xattr on Unix, ADS on Windows).
- Storage keys are sharded: `prefix/v1aabbccdd` → `prefix/v1/aa/bbccdd`.
- `write_once` config is an object: `{"action": "allow"|"deny", "notification": "never"|"always"|"content_differs"}`. Defaults: `action=allow`, `notification=never`.
- HTTP Basic Auth with `username`/`password` credentials. To run without auth, set `disable_auth: true` (and omit `credentials`). The old empty-string convention for disabling auth has been removed — empty strings in a credential entry are now a config error.
- `credentials` config is required unless `disable_auth: true`. Each entry needs both `username` and `password` set to non-empty values.
- String config values support env var references: `{"type": "envvar", "name": "VAR_NAME"}` resolves to `os.Getenv("VAR_NAME")` at load time. Used via the `ConfigString` type. An env var that resolves to `""` is rejected the same as a literal empty string, so an unset env var cannot silently disable auth.
- Every `PutObject` persists audit metadata as extended attributes on the stored file: `user.s3audit.uploader`, `user.s3audit.uploaded_at`, `user.s3audit.client_ip`, `user.s3audit.user_agent`, `user.s3audit.content_length`. Namespace `user.s3audit.*` is separate from user-supplied `X-Amz-Meta-*` (stored as `user.s3.*`) so user metadata cannot spoof audit fields. On Windows, audit is a JSON sidecar at `<path>.audit`.
- Client IP is resolved via `CF-Connecting-IP` → `X-Real-IP` → first entry of `X-Forwarded-For` → `r.RemoteAddr`. These proxy headers are trusted unconditionally; deploy only behind a trusted reverse proxy.
- Cache version: `storage.go` stamps the `data_dir` with a `.cache_version` file. `currentCacheVersion` is a constant in `storage.go`. On startup, if the stored version (or 1, if the marker is missing) differs from current, every entry in `data_dir` is removed before serving. Bump `currentCacheVersion` to force operators to rebuild the cache after a change that should invalidate prior contents.
- `eviction` config is an object: `{"max_age": "720h", "max_bytes": 0, "interval": "72h"}`. `interval` defaults to 3 days (`defaultEvictionInterval`) — infrequent on purpose, since a 30-day age window gains nothing from frequent sweeps; shorten it only for a tight `max_bytes`. `max_age`/`interval` are Go duration strings (or seconds as a number) parsed by the `Duration` type in `config.go`; `max_bytes` is an int. `max_age` is a `*Duration` so an absent field takes the default (`defaultEvictionMaxAge`, 30 days) while an explicit `"0"` disables it — `max_bytes` defaults to `0` (off). Eviction is **on by default**; both off → unbounded growth + a startup warning. Eviction is driven by *last use* (`max(mtime, lastAccess)`), never by rewriting mtime, so the prefetch grouping that keys on mtime is unaffected. Metrics: `s3_evictions_total`, `s3_evicted_bytes_total`, `s3_cache_bytes`.
