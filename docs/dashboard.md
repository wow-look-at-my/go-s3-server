# The operator dashboard

The server serves one HTML page that shows what the cache does. It reports hit rate, size against budget, batch volume, the tripwire counters, memory, and the running configuration.

The page listens on its **own port**, apart from the cache protocol port. An access proxy publishes that port to people. The port the build machines use stays untouched.

## Configuration

Set `dashboard_listen` in the config file, or `--dashboard-listen` on the command line.

| Value | Result |
| --- | --- |
| absent | the dashboard listens on `:9002` |
| `"127.0.0.1:9002"` | the dashboard listens there |
| `""` | no dashboard, and no extra port is opened |

The command-line flag cannot pass an empty value. An unset flag is empty too. So `--dashboard-listen off` is the spelling that stops the dashboard from the command line.

A bind failure logs and the cache keeps serving. The data path must not die because the dashboard's port is busy.

## What it serves

| Path | Answer |
| --- | --- |
| `/` | the page |
| `/dashboard.css`, `/dashboard.js` | its two assets |
| `/api/stats` | the JSON snapshot the page polls every 5 seconds |
| `/_health` | `200 ok` |

The page and its assets are embedded in the binary. There is no asset directory to mount. The page cannot disagree with the server that serves it.

## Where the numbers come from

`/api/stats` flattens the same Prometheus registry that `/metrics` serves. The dashboard keeps no counters of its own. The page and a scrape can never report different values for one counter.

A counter with no labels becomes one number. A labeled one becomes a series, keyed by its label set. A histogram contributes `<name>_count` and `<name>_sum`, which is what an average needs. The buckets stay in `/metrics` for a time-series database to read.

The page reports these carefully:

- **Cache size** comes from `s3_cache_bytes`. The server writes that gauge at an eviction sweep and every 15 minutes, not on every PUT. Before the first measurement the page says "not measured". It never says `0 B`.
- **Rates** are computed in the browser, from one poll against the poll before it. A reload starts them over. A counter that goes backwards means the server restarted, so the page clears the history instead of drawing a negative rate.

The snapshot reports configuration, never credentials. `TestDashboardStatsCarryNoCredentials` holds that line.

## Authentication: there is none, and that is the point

The dashboard port answers every request. Publish it through an access proxy that does the identity check. Cloudflare Zero Trust is one such proxy. Do not expose the port directly.

### Cloudflare Zero Trust

Use one tunnel, with a hostname per port of this process. Only the dashboard hostname gets an Access policy.

```yaml
# cloudflared config.yml
tunnel: <tunnel-id>
credentials-file: /etc/cloudflared/<tunnel-id>.json
ingress:
  # The dashboard: an Access application sits in front of it, so a browser
  # must pass the identity provider to reach this port.
  - hostname: cache-dashboard.example.com
    service: http://go-cache:9002
  # The cache protocol: no Access policy. Build machines carry basic-auth
  # credentials, not browser sessions, and an Access login page cannot be
  # answered by a Go toolchain.
  - hostname: cache.example.com
    service: http://go-cache:9000
  - service: http_status:404
```

Then add a self-hosted Access application for `cache-dashboard.example.com` in Zero Trust. Give it whatever policy suits: an email domain, or a group.

Leave `cache.example.com` without an Access policy. Access in front of the cache port breaks every build, because the client cannot complete a browser login.

`cloudflared` often runs in its own container. The dashboard must then bind an address that container reaches. `:9002` binds every interface and works. `127.0.0.1:9002` does not.

`CF-Connecting-IP` is already the first header the cache port trusts for the client IP. A tunnelled deployment therefore logs real client addresses.
