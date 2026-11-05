# The operator dashboard

The server serves one HTML page that shows what the cache does. It reports hit rate, size against budget, batch volume, why the cache missed, what changed between multiple uploads of one package. The tripwire counters, memory, and the running configuration.

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
| `/api/stats` | the JSON snapshot the page polls every second; the download json button saves the one on screen as a file |
| `/_health` | `200 ok` |

The page and its assets are embedded in the binary. There is no asset directory to mount. The page cannot disagree with the server that serves it.

## Where the numbers come from

`/api/stats` flattens the same Prometheus registry that `/metrics` serves. The dashboard keeps no counters of its own. The page and a scrape can never report different values for one counter.

A counter with no labels becomes `{"value": N}`. A labeled one becomes `{"series": [{"labels": {"method": "GET", "route": "Index"}, "value": N}, ...]}`, with one entry per label set. A reader looks a label up by name and never parses a key. A histogram contributes `<name>_count` and `<name>_sum`, which is what an average needs. The buckets stay in `/metrics` for a time-series database to read.

The page reports these carefully:

- **Cache size** comes from `s3_cache_bytes`. The server writes that gauge at an eviction sweep and every few minutes, not on every PUT. Before the first measurement the page says "not measured". It never says `0 B`.
- **Rates** are computed in the browser, from one poll against the poll before it. A reload starts them over. A counter that goes backwards means the server restarted, so the page clears the history instead of drawing a negative rate.

## Reads the server never sees

The client keeps a copy of `/_index`. When that copy is fresh, the client misses a key the copy lacks with no request. It also skips the upload of a key the copy holds. The server cannot count either operation by itself, so its hit rate counts only the keys that reached it.

The client therefore reports those skips. It sends `POST /{bucket}/_skips` from a skip that comes at least `skipReportInterval` after the last send, and again at `Close`. That `POST /{bucket}/_skips` is with the counts since the last report the server accepted. A failed report leaves the counts in place for the next one. The code is `cacheclient/skipreport.go` and `skips.go`.

The server counts each report in `s3_client_skips_total{op,reason}`, and the uploads the client lost in `s3_client_uploads_lost_total{reason}`. Each skipped get is also a miss in `s3_project_objects_total` for the project that asked. The hit rate tile divides `found` plus the single-GET hits by `requested` plus the skipped gets plus the single GETs. The hit chart takes the same ratio over the keys asked since the previous poll. A drop shows at once and does not fade into the lifetime average.

A server older than the endpoint answers 405. The client then stops reporting for that run and logs one warning, because that server's hit rate stays too high.

## The BY PROJECT panel

A shared cache gets one question more than any other. The hit rate reads fine overall, so whose builds are the ones missing?

The panel answers it from `s3_project_objects_total{project,kind}`. The chart is a stacked area: one band per project, each column the total objects per second the cache moved for it. The total and the split that makes it up therefore read off one picture. The table beside it carries each project's hit rate, `hit / (hit + miss)`, worst first. The chart cannot show that. A project can be a thin band and still hit almost nothing it asks for.

Every kind counts for the project that sent the request, the `X-Cache-Module` header. A miss has no object. The requester is the only name it can carry. A hit therefore uses the requester too, not the module stored on the object. That stored module names the build that first wrote the object. A shared std archive will credit every later build's hit to that writer.

The row will divide one project's hits by another project's misses. A request with no header counts under `unknown` for every kind. `projectOf` in `logagg.go` holds the rule.

A look-ahead request (`prefetch_only`) names keys the client has just hit, as anchors for the window around them. It asks for none of them. Those keys count as `s3_batch_keys_total{kind="anchors"}`,never as `requested` or as a project miss. Counted as misses, each hit will also read as a miss.

The bands come from `<perf-graph>`'s stacked mode, which the page fetches from the js-snippets library site at run time. This page can therefore be newer than the component it loaded. When the loaded component has no `pushSeries`, the page says so under the chart. A stacked chart that silently stayed blank reads as "no traffic".

Name orders bands, not by traffic. A band that reorders itself on every poll cannot be followed. Each band's color comes from its own name, so one project is one color on every machine.

The panel reads the snapshot's `projects` map, not the Prometheus series. That map keeps an exact row for every project name, with no bound. The Prometheus series are bounded, because the project name arrives on a client header and each label value is a series a scraper keeps. `projectLabeller` bounds them at `maxProjectLabels` in `projectmetrics.go`. A name past it folds into `other` and increments `s3_project_names_folded_total`. That fold changes only the scraped metrics, not the panel.

The snapshot reports configuration, not credentials. `TestDashboardStatsCarryNoCredentials` holds that line.

## The WHY MISSES panel

A hit rate says how frequently the cache missed. This panel says why. It reads the snapshot's `misses` section, which `diagnoseMisses` in `missdiag.go` builds from the same registry as everything else.

Every miss the server knows of lands in one cause. The causes come from multiple places:

| Source | Where the count comes from | Causes |
| --- | --- | --- |
| client | `s3_client_skips_total{op="get",reason}`, the skip report | never stored (`not_in_index`), no index yet (`empty_index`), client gave up probing (`batch_backoff`), asked before this run (`known_miss`) |
| server | `s3_batch_misses_total{reason}` plus the matching `s3_get_requests_total{outcome="miss_<reason>"}` | unknown key (`not_found`), evicted since the client's index (`evicted`), advertised but unservable (`advertised_unservable`) |
| integrity | the same two counters | module index refused on read, read guard could not inspect the body, body cannot be repaired, body changed on disk, stat failed |

A batch GET used to count a requested key it can not answer as one number, requested minus found. `servableEntry` now reports why each one was left out, and `openVerifiedEntries` reports a body that failed its digest check after the stat. A prefetch candidate that is dropped is not a miss and is not counted.

The `evicted` cause needs a memory the server did not have. `Storage.evicted` is a byte-bounded LRU of the action hashes the sweeper removed, filled by `evictOne` and cleared for a key by `PutStream`. `absentKeyReason` asks it after the index: an advertised key is divergence. A remembered victim is a capacity miss, and anything else was never stored here. The memo is registered with the memory controller under `evicted-keys`. It shrinks with the other caches, and a victim old enough to fall out of it reads as unknown again.

Each cause carries a severity. An `expected` miss means the cache works and the key was never there. A `capacity` miss means the cache held the key and let it go. A `bug` means the cache must have served the key. The verdict names the dominant cause with its share and the action for it. A bug with any count is named beside it, because an expected miss needs nobody and a bug needs somebody.

The page adds the thing the server cannot know: how many of each cause arrived since the last poll. A cause that fired once at startup and a cause that is still firing read the same in a lifetime count.

The supply-side signals under the causes are the ways an upload is lost. The cache size against its budget, objects evicted, requests shed, uploads refused or rejected or failed, and metadata dropped. An object never stored is a miss on every later build.

The client reports its own lost uploads in the skip report. `put_failed` is an upload it sent and the store never confirmed. `put_refused` is one its own guards stopped. `put_abandoned` is one it never sent. The server counts them in `s3_client_uploads_lost_total{reason}`, shows them as the first signals, and names the failed and abandoned ones in the verdict. `Close` sends the report last, after every upload drained or was handed off, on a context the hand-off does not cancel.

A process does not exit between its last upload and the report that says what happened to it.

A client that skips a key its index copy lacks cannot know whether the sweeper evicted it or nothing ever stored it. The server can. The report carries a sample of the skipped keys, up to `skipSampleMax` per report, and the server says what it knows of each. `evicted` (the sweeper removed it since the client fetched its index), `advertised` (the server holds it now, so the client's index copy was older than the upload), or `unknown`. The counts land in `s3_client_skip_samples_total{verdict}`. The never-stored row explains the sample's split, turns into a capacity miss when most sampled keys were evicted.

The verdict then says to grow the cache.

## The WHAT CHANGED panel

A build that stores a package the cache already held under another key compiled it again. The hit rate reads that as a miss. This panel says why the key moved.

The action id hashes every input, and the server sees none of them. It sees what the client said about each upload. The package path, the Go version, the client version, the target, the first source file names, and the output id. `churnTracker` in `churn.go` keeps the last upload or hit of every package, keyed by package and target. That `churnTracker` is in a byte-bounded LRU registered with the memory controller as `package-churn`. A put of a known package under a new key is a re-store.

The first field that differs names its kind:

| Kind | What differed | What it means |
| --- | --- | --- |
| `go_version` | the Go version | a new compiler recompiles everything once |
| `toolchain` | the go-toolchain client version | a client release that carries a new compiler build changes every action id |
| `target` | GOOS/GOARCH | a fleet that builds for several targets |
| `sources` | the source file names | a file added or removed |
| `inputs` | nothing, and the output id is the same | an input the metadata does not carry: a flag, an environment variable, a stamp, a path, a dependency's build id |
| `output` | nothing, and the output id differs | a file's content, or a dependency whose output changed |

cmd/go stamps the action id into every archive it compiles, so the stored bytes of multiple keys always differ. The output id can never say "same". The build id in the archive carries a second field, the content hash, computed with the stamp zeroed. The client sends it as `content-id`, and the server compares that. Consider an upload from a client older than the field. That upload answers `unknown`, which the panel says beside the re-store rather than reading it as changed content.

A package named `main` or `command-line-arguments` is not tracked. Every test main and every directory build carries that import path, so one name will stand for thousands of packages.

The `inputs` kind is the pointless re-store. Nothing visible changed and the compiled bytes are same. The build still can not find the object. The panel colours it, ranks the packages stored again most frequently, and lists the latest re-stores with each differing field as `from -> to`. The verdict names the dominant kind with its newest example.

A hit teaches the tracker the newest object the cache served, whoever stored it. A put then compares against that. The tracker starts empty at every server start and fills as builds run. `s3_package_restores_total{change,output}` counts the same events for a scrape, and each project's row carries `restores` and `restores_same_output`.

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

`cloudflared` frequently runs in its own container. The dashboard must then bind an address that container reaches. `:9002` binds every interface and works. `127.0.0.1:9002` does not.

`CF-Connecting-IP` is already the first header the cache port trusts for the client IP. A tunnelled deployment therefore logs real client addresses.
