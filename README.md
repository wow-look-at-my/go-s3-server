# go-s3-server

Minimal S3-compatible server backed by the local filesystem. Designed as a shared build cache for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain).

## Features

- **S3 API subset** — `GetObject`, `PutObject`, `ListObjectsV2`
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
  "write_once": {"action": "deny", "notification": "content_differs"}
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

### `write_once` options

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `action` | `allow`, `deny` | `allow` | Whether to allow overwriting existing keys |
| `notification` | `never`, `always`, `content_differs` | `never` | When to return HTTP 409 on overwrite attempts |

- `action: "deny"` + `notification: "never"` — silently skip overwrites (200 response)
- `action: "deny"` + `notification: "always"` — reject any overwrite attempt (409 response)
- `action: "deny"` + `notification: "content_differs"` — reject only when content differs; same content is idempotent (ideal for content-addressable caches)

## Authentication

The server has no built-in authentication. Use a reverse proxy (e.g. traefik) in front of it to handle auth.

```bash
# PUT an object
curl -X PUT --data-binary @file.bin http://localhost:9000/my-cache/path/to/key

# GET an object
curl http://localhost:9000/my-cache/path/to/key

# List objects
curl 'http://localhost:9000/my-cache?list-type=2&prefix=path/'
```

## Docker

```
docker run -v /data:/data ghcr.io/wow-look-at-my/go-s3-server --config /data/config.json
```

## Building

This project uses [go-toolchain](https://github.com/wow-look-at-my/go-toolchain). Run from the project root:

```
go-toolchain
```
