# Staying inside the memory budget

An OOM kill is the worst failure this server has. Every in-flight upload and
download dies at once, the client sees a 502 from the proxy rather than
anything it can retry sensibly, and the next build starts against a cache that
just lost whatever was in flight. A 503 with `Retry-After`, by contrast, costs
one client two seconds.

So the server is built to shed rather than die, and everything below exists to
make that the outcome under load.

## What actually grows

"Bodies are streamed" bounds the cost of ONE request, not the total. The
process's memory is:

| what | grows with | bounded by |
|---|---|---|
| the key index (`index.go`) | the number of cached keys | nothing — it is the served data |
| the serialized `/_index` blob | keys × 32 B (~32 MB at 1M keys) | dropped under pressure, rebuilt on demand |
| metadata cache (`metacache.go`) | the working set | entry bound, sized from the budget |
| known-clean memo (`cleanmemo.go`) | the working set | entry bound, sized from the budget |
| prefetch tracker (`batch.go`) | recent prefetch traffic | 5-minute TTL; dropped under pressure |
| in-flight requests | concurrency × per-request working set | the concurrency limit, capped by the budget |
| one batch manifest | keys per batch | `maxBatchKeys`, sized from the budget |

Before this existed, every one of those bounds was a constant chosen for a
large host. On a smaller one the process simply grew until the kernel killed
it.

## The budget

`detectMemoryBudget` (`memlimit.go`) asks the runtime first:
`debug.SetMemoryLimit(-1)` returns `GOMEMLIMIT` if the operator set it, or the
value go-toolchain's injected cgroup guard installed at startup (it reads the
cgroup v2/v1 limit and applies a 0.9 ratio). Reading the runtime's own number
means the server and the GC agree on one ceiling instead of computing two.

Failing that it reads `/sys/fs/cgroup/memory.max` and the v1 equivalent, for a
binary built without that guard.

**An undiscoverable budget is 0, and 0 disables everything in this file.** The
caches keep their old fixed sizes, the concurrency limit is untouched, nothing
is ever shed, and startup logs a warning saying so. An unknown limit must not
become an invented one.

## What the budget changes

**Sizing, at startup.** `budgetFraction` gives each cache a share of the budget,
floored so it stays useful and capped at the constant it used to be — so a
large host behaves exactly as before and a small one gets smaller caches
instead of an OOM. `concurrencyForBudget` caps `max_concurrent_requests` so
admitted requests × their worst-case working set fits the budget; it only ever
lowers a configured value, and never below 8.

**Trimming, then shedding, at runtime.** `memWatcher` samples every 250 ms what
the runtime counts against its limit (`/memory/classes/total:bytes` minus
`/memory/classes/heap/released:bytes` — the same quantity `GOMEMLIMIT`
governs, unlike a heap-only figure that misses stacks and metadata). Then:

- **≥ 80 %** — trim. Every registered trimmer drops rebuildable state (the
  metadata cache, the known-clean memo, the cached index blob, the prefetch
  tracker) and memory is returned to the OS. Rate-limited to once per 30 s,
  because a trim drops warm caches and forces a collection. Correctness never
  depends on any of it; the cost is re-reading.
- **≥ 92 %** — shed. New requests get `503` + `Retry-After: 2` before they take
  a concurrency slot. The health endpoint is answered before this check, so an
  orchestrator still sees a live instance and routes to it again the moment
  pressure passes.
- **GC CPU ≥ 25 % / 50 %** — trim / shed, on the same ladder. This is the shape
  the memory gauge cannot see: Go does not OOM when the working set will not
  fit, it collects harder, so memory in use can sit comfortably below the
  ceiling while the process spends its time collecting instead of serving. In
  the loads measured below the memory thresholds always fired first; this is a
  backstop.

The order is the point: trimming costs a re-read, shedding costs a client its
request, and dying costs everyone theirs.

## Measured

The shipped binary at `GOMEMLIMIT=16MiB` -- deliberately far below what the
workload needs -- against 240 uploads of 8 MiB each, 12 concurrent:

```
elapsed=4s : 240 PUTs x 8 MiB, 12 concurrent, 16 MiB budget
     93 200
    147 503
SERVER ALIVE          VmHWM: 26 MB
s3_memory_in_use_bytes 16153616   (budget 16777216)
s3_memory_shed_total   147
s3_memory_trims_total  1
```

Every request got an answer: 93 stored, 147 told to come back in two seconds.
The process stayed alive and serving throughout, and the same budget had
already lowered the configured concurrency limit from 128 to 16 at startup.

The same load at `GOMEMLIMIT=128MiB` is 360/360 `200`, nothing shed, no trims --
the machinery is invisible until it is needed.

## Operating it

- `s3_memory_limit_bytes` — the discovered ceiling. **A zero here means the
  server cannot protect itself**, and is worth alerting on in a container that
  is supposed to have a limit.
- `s3_memory_in_use_bytes` — the last sample.
- `s3_memory_trims_total` — trims. A slow trickle is the system working; a
  steady climb means the working set genuinely does not fit and the container
  wants more memory.
- `s3_memory_shed_total` — requests refused for memory. Nonzero means trimming
  was not enough. This is the metric that should stay at zero in steady state.
- `s3_memory_gc_cpu_fraction` — the share of CPU the collector took over the
  last interval. Sustained high values with memory looking fine is the
  "collecting instead of serving" shape; the container wants more memory.

To give the server more room, raise the container's memory limit (the guard
picks it up automatically) or set `GOMEMLIMIT` explicitly. To reproduce
small-container behavior locally, run with `GOMEMLIMIT=256MiB`.
