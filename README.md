# go-s3-server

Minimal S3-compatible server backed by the local filesystem. Designed as a shared build cache for [go-toolchain](https://github.com/wow-look-at-my/go-toolchain).

## Features

- **S3 API subset** — `GetObject`, `PutObject`, `ListObjectsV2`
- **AWS Signature V4** authentication
- **Write-once mode** — skip overwriting existing keys (ideal for content-addressable caches)
- **Sharded storage** — keys are automatically split into a two-level directory tree to avoid huge flat directories
- **Multi-arch Docker image** — `linux/amd64` and `linux/arm64` published to `ghcr.io/wow-look-at-my/go-s3-server`

## Quick start

Create a JSON config file:

```json
{
  "listen": ":9000",
  "bucket": "my-cache",
  "region": "us-east-1",
  "data_dir": "/var/data/s3",
  "write_once": true,
  "credentials": [
    {
      "access_key": "AKID",
      "secret_key": "SECRET"
    }
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
| `--region` | Override region |
| `--data-dir` | Override data directory |

All flags except `--config` override the corresponding config file value.

## Config reference

| Field | Type | Default | Required | Description |
|-------|------|---------|----------|-------------|
| `listen` | string | `:9000` | no | Address to listen on |
| `bucket` | string | — | yes | S3 bucket name to serve |
| `region` | string | `us-east-1` | no | AWS region for signature verification |
| `data_dir` | string | — | yes | Directory to store objects |
| `write_once` | bool | `false` | no | Skip writes if the key already exists |
| `credentials` | array | — | yes | At least one `access_key`/`secret_key` pair |

## Docker

```
docker run -v /data:/data ghcr.io/wow-look-at-my/go-s3-server --config /data/config.json
```

## Building

This project uses [go-toolchain](https://github.com/wow-look-at-my/go-toolchain). Run from the project root:

```
go-toolchain
```
