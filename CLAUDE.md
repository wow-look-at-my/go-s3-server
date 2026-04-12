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
- `server.go` — HTTP router, bucket dispatch
- `handlers.go` — S3 API handlers (GetObject, PutObject, ListObjectsV2)
- `storage.go` — Filesystem storage with two-level key sharding
- `storage_unix.go` / `storage_windows.go` — Platform-specific file locking and xattr metadata
- `lock_windows.go` — Windows file locking via syscall
- `handlers_test.go` — Unit tests for handlers
- `.github/workflows/ci.yml` — CI pipeline (build, docker, s3-api-test, integration test)
- `.github/workflows/integration-test/` — Integration test harness (configs, test Go project)

## Conventions

- CLI parsing uses cobra. The single root command is in `main.go`.
- S3 responses are XML-encoded using `encoding/xml`.
- Object metadata is stored in filesystem extended attributes (xattr on Unix, ADS on Windows).
- Storage keys are sharded: `prefix/v1aabbccdd` → `prefix/v1/aa/bbccdd`.
- `write_once` config is an object: `{"action": "allow"|"deny", "notification": "never"|"always"|"content_differs"}`. Defaults: `action=allow`, `notification=never`.
- No built-in authentication — use a reverse proxy (e.g. traefik) for auth.
