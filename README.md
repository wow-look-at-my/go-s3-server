# go-s3-server

Minimal S3-compatible server backed by the local filesystem. Designed as a shared build cache for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain).

## Features

- **S3 API subset** — `GetObject`, `PutObject`
- **Cache-key index** — `GET /<bucket>/_index` returns a precomputed binary blob (GBCI v1) of every cacheprog action-ID hash, with strong ETag and `If-None-Match` 304 support
- **HTTP Basic Auth** — multiple users, or explicitly disable with `disable_auth: true`
- **Write-once mode** — deny overwriting existing keys with configurable conflict notification (ideal for content-addressable caches)
- **Sharded storage** — keys are automatically split into a two-level directory tree to avoid huge flat directories
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

## Docker

```
docker run -v /data:/data ghcr.io/wow-look-at-my/go-s3-server --config /data/config.json
```

## Building

This project uses [go-toolchain](https://github.com/wow-look-at-my/go-toolchain). Run from the project root:

```
go-toolchain
```
