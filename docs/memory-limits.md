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
| last-access records (`eviction.go`) | keys read since startup | **no** — but only exists on a `noatime` data_dir; see `docs/eviction.md` |
| in-flight requests | concurrency | **no** — but bodies stream, so each is small |

The first three are byte-bounded LRU caches (`lrucache.go`): each holds a
budget's worth of bytes and evicts its least-recently-used entries to stay
there. Everything they hold is reconstructible from disk, which is precisely
why they are the right thing to give up.

The index is not droppable, and that sets the floor: a container has to be big
enough to hold the index for the cache it serves. Below that no amount of
eviction helps, and the server says so in the log rather than pretending
otherwise.

## The index is the floor, so it is stored in bytes, not strings

A production instance holding 1.11M keys sat at 645 MB in use against a 1 GiB
container, with 432 MB of live heap and 1.29M heap objects — about one
allocation and ~388 bytes per indexed key. The three shrinkable caches held
19 MB between them, 3% of the total, so the controller shrinking them to their
floor released ~20 MB while the process stayed at 645 MB. It was defending the
wrong 3%.

The size was in the key strings. Every cacheprog key is the constant
`go-buildcache/v1` plus a 64-character hex action ID: 80 bytes of string, on
its own heap object, carrying 32 bytes of entropy — held once per index entry,
once per eviction candidate and once per access record. `compactKey`
(`compactkey.go`) stores the 32 bytes inline instead and rebuilds the string
only for the keys a caller actually receives. What the index costs per key is
now the sum of three fixed structures, and nothing else:

| structure | bytes per key |
|---|---|
| `indexEntry` (mtime-ordered, for prefetch) | 56 |
| the sorted action-ID master list | 32 |
| the serialized `/_index` blob it publishes | 32 |

That is ~120 bytes per key, or ~130 MB at a million keys, with no per-key
allocation for the GC to trace. `TestCompactKeyCostsNothingPerKey` pins both
properties.

The other half was periodic rather than resident: rebuilding the index and
sweeping for eviction both materialized the whole cache as a `[]ListObject`
with a key string apiece (~140 MB at a million keys), and the sweep then built
a candidate list and a live-key set on top of that. `Storage.Walk` now hands
each object to a callback as the directory walk finds it, and both callers
consume it into compact structures — the sweep into a bounded histogram and
bounded victim batches, so its peak is a batch rather than the cache. The
rebuild also stopped routing walked hashes through the pending buffer, which
had left a full-size backing array alive for the life of the process.

## The budget

`detectMemoryBudget` (`memlimit.go`) asks the runtime first:
`debug.SetMemoryLimit(-1)` returns `GOMEMLIMIT` if the operator set it, or the
value go-toolchain's injected cgroup guard installed at startup (it reads the
cgroup v2/v1 limit and applies a 0.9 ratio). Reading the runtime's own number
means the server and the GC agree on one ceiling instead of computing two.
Failing that it reads `/sys/fs/cgroup/memory.max` and the v1 equivalent — which
is a fallback for a binary built without the guard, not the normal path.

Each cache gets a fraction of that as its fully-grown budget (metadata 10%,
known-clean 3%, prefetch 2%). **An undiscoverable budget is 0**, and then the
caches use fixed defaults and the controller does not run at all — an unknown
limit must not become an invented one.

### Resolve it after init(), not before

The GC's own ceiling is set for us: go-toolchain injects a `gomemlimit_gen.go`
into every main package it builds, whose `init()` reads the cgroup limit and
calls `debug.SetMemoryLimit(0.9 × limit)` unless `GOMEMLIMIT` is already set.
This server must not install a second one — it would be dead code re-deriving
the same number.

What it must do is *read* the right number, and that is a matter of ordering.
`detectMemoryBudget` used to run from a package-level variable initializer, and
Go runs every package-level initializer **before** any `init()`. So it ran
before the guard, found no runtime limit, fell back to `/sys/fs/cgroup/memory.max`
and reported the raw container limit — 1 GiB where the GC was enforcing 966 MB.
The caches were sized against a ceiling nothing enforced, and
`s3_memory_limit_bytes` published it. That gauge reading exactly the container
limit is what "GOMEMLIMIT is not set" looks like from the outside, and it sent
two separate investigations down that path.

`resolveMemoryBudget` is therefore called from `run()`, by which point every
`init()` has finished, and `debug.SetMemoryLimit(-1)` returns the ceiling the GC
is actually using.

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
