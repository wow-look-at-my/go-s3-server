# Staying inside the memory budget

The rule this file exists to enforce: **the server holds less, it never serves
less.**

A cache exists to answer. A cache that refuses under load is worse than no
cache, because every client it turns away rebuilds anyway — having first paid
for the round trip — and a client cannot tell "busy" from "broken". So memory
pressure here has exactly one consequence: the in-memory caches get smaller.
Requests are untouched, and no code on the request path so much as reads the
memory state.

## What is in memory, and what may be dropped

| what | grows with | may be dropped? |
|---|---|---|
| metadata cache (`metacache.go`) | keys recently read | **yes** — a re-read costs ~12 syscalls |
| known-clean memo (`cleanmemo.go`) | keys recently read | **yes** — a re-probe costs one open |
| prefetch suppression (`batch.go`) | recent prefetch traffic | **yes** — worst case one pool is re-sent |
| the key index (`index.go`) | keys stored | **no** — it *is* what `/_index` serves |
| in-flight requests | concurrency | **no** — but bodies stream, so each is small |

The first three are byte-bounded LRU caches (`lrucache.go`): each holds a
budget's worth of bytes and evicts its least-recently-used entries to stay
there. Everything they hold is reconstructible from disk, which is precisely
why they are the right thing to give up.

The index is not droppable, and that sets the floor: a container has to be big
enough to hold the index for the cache it serves (roughly 130 bytes per key —
about 130 MB at a million keys). Below that no amount of eviction helps, and
the server says so in the log rather than pretending otherwise.

## The budget

`detectMemoryBudget` (`memlimit.go`) asks the runtime first:
`debug.SetMemoryLimit(-1)` returns `GOMEMLIMIT` if the operator set it, or the
value go-toolchain's injected cgroup guard installed at startup (it reads the
cgroup v2/v1 limit and applies a 0.9 ratio). Reading the runtime's own number
means the server and the GC agree on one ceiling instead of computing two.
Failing that it reads `/sys/fs/cgroup/memory.max` and the v1 equivalent.

Each cache gets a fraction of that as its fully-grown budget (metadata 10%,
known-clean 3%, prefetch 2%). **An undiscoverable budget is 0**, and then the
caches use fixed defaults and the controller does not run at all — an unknown
limit must not become an invented one.

## The controller

`memController` samples every 250 ms what the runtime counts against its limit
(`/memory/classes/total:bytes` minus `/memory/classes/heap/released:bytes` —
the quantity `GOMEMLIMIT` governs, unlike a heap-only figure that misses stacks
and metadata), and moves one number: the scale applied to every cache budget.

- **≥ 85 % in use** — halve the scale, which evicts, then return the freed
  memory to the OS. At most once every 2 s, so one burst does not walk the
  caches to their floor.
- **≤ 65 % in use** — grow the scale back by 25 %, at most once every 30 s, up
  to full. Slow on purpose: a transient burst should not leave the server
  permanently cold, and a fast recovery would just re-create the pressure.
- **at the floor (1/32 of full) and still under pressure** — stop, and log once
  that the caches have nothing left to give and the container needs more
  memory. That is the one case the server cannot fix by itself, so it says so
  instead of thrashing.

The gap between 85 % and 65 % is hysteresis; without it the scale would
oscillate on every sample.

## Measured

The shipped binary at `GOMEMLIMIT=16MiB` — far below what the load needs —
against 240 uploads of 8 MiB each, 12 concurrent:

```
elapsed=12s : 240 PUTs x 8 MiB, 12 concurrent, 16 MiB budget
    240 200
SERVER ALIVE          VmHWM: 25 MB
s3_memory_in_use_bytes   11951120   (budget 16777216)
s3_memory_shrinks_total  5
s3_cache_memory_budget_bytes{cache="metadata"} 65535   (4% of full)
s3_http_rejected_total   0
```

Every request answered. The server absorbed a load worth 1.9 GB of uploads
inside a 16 MiB budget by shrinking its caches five times — from 1.6 MB of
metadata cache down to 64 KB — and served normally throughout. Nothing was
refused, delayed, or dropped.

## Operating it

- `s3_memory_limit_bytes` — the discovered ceiling. Zero means none was found,
  so the caches are on fixed defaults; worth alerting on in a container that is
  supposed to have a limit.
- `s3_memory_in_use_bytes` — the last sample.
- `s3_memory_shrinks_total` — how often pressure has forced the caches down. A
  slow trickle is the system working. A steady climb means the working set does
  not fit and the cache is running cold: give the container more memory.
- `s3_cache_memory_bytes{cache}` / `s3_cache_memory_budget_bytes{cache}` — what
  each cache holds and is allowed to hold. Budgets far below full is the
  visible signature of sustained pressure.

There is deliberately no metric for "requests refused due to memory", because
there is no such thing.
