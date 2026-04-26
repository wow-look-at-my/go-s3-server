# go-s3-server

Minimal S3-compatible server for use as a shared Go build cache.

## Build & test

```
go-toolchain
```

Do NOT use `go build`, `go test`, or any bare `go` commands. Always use `go-toolchain` from the project root.

## Project layout

- `main.go` — CLI entry point (cobra)
- `config.go` — JSON config loading and validation
- `auth.go` — HTTP Basic Auth; returns the authenticated username
- `auth_test.go` — Auth tests (including the "empty credential must not bypass auth" regression)
- `server.go` — HTTP router, auth gate, client-IP resolution, audit context, bucket dispatch
- `handlers.go` — S3 API handlers (GetObject, PutObject) plus `handleGetIndex` for the `/_index` endpoint; PutObject writes audit xattrs
- `index.go` — In-memory key index. Maintains an mtime-sorted list for `_batch/get` prefetch *and* a sorted slice of action-ID hashes that serializes to the GBCI v1 binary blob served at `GET /<bucket>/_index`. PUTs append to an unsorted `pending` buffer under a microsecond-scale mutex; sort+dedupe+serialize is deferred to the next `Blob()` read.
- `storage.go` — Filesystem storage with two-level key sharding, cache version auto-purge
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
