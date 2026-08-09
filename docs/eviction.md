# Eviction: an LRU bounded by size

The cache is bounded by **size**, and when it is over budget the **least
recently used** entries go first. It is not a TTL: a build-cache entry that is
still being read is still useful however long ago it was written, and dropping
it costs a rebuild for nothing.

Age eviction still exists (`eviction.max_age`) and is off by default. It is a
second, independent cutoff for operators who want one.

## What "last used" means

The latest of three signals, so a missing one can only make an entry look
older, never younger (`lastUsedUnix` in `atime.go`):

1. **mtime** — when the body was written.
2. **The filesystem's access time**, which the kernel advances whenever a body
   is read. Under the default `relatime` it moves at most once a day, which is
   the resolution a multi-day eviction window needs, and — the point — it
   **survives restarts**.
3. **This process's own record** of reads since startup (`accessShards`).

Before atime was consulted, only 1 and 3 existed, and 3 starts empty on every
boot. A rolling update therefore erased every read the server knew about, and
the next sweep saw a cache full of entries whose only timestamp was a write
from months ago — and evicted things that were being read constantly. That is
the bug this list exists to fix.

At startup the server **probes** the real data_dir rather than guessing from
mount options (`atimeIsRecorded`): write a throwaway file, backdate it, read it
back, and see whether the access time moved. That answers for the actual
filesystem, which `noatime`, overlayfs, bind mounts and NFS all make hard to
infer. When the answer is no, the server logs a warning naming the consequence
and enables the in-memory map instead — which is correct while it runs, empty
after each restart, and one entry per key read, the memory the probe otherwise
saves.

An internal peek (the module-index guard, the self-heal's hashing handle) reads
a body too, so it advances atime for a prefetch candidate that was never sent.
It is bounded — those paths are memoized per key — and the effect is that a
candidate the server considered looks used for a while. That is a small, known
imprecision, not a silent one.

## A sweep, in two walks

A sweep never builds a list of the cache's contents. At a million objects that
list — one heap-allocated key string apiece — was the largest allocation this
process made, and it was made on a schedule (see `docs/memory-limits.md`).

**Walk 1 (`scanForEviction`)** measures: total bytes, and a histogram of bytes
by last-use time in 10-minute buckets. The histogram is bounded by the *spread*
of last-use times, not by the object count.

From it the sweep derives ONE number, the **cutoff**: evict everything last
used before it.

- The age cutoff is `now - max_age`, or 0 when age eviction is off.
- The size cutoff walks buckets oldest-first, accumulating bytes until the
  running total covers `total - max_bytes`, and takes that bucket's upper edge.
- The two combine as `max(ageCutoff, sizeCutoff)` — both passes are the same
  rule at different cutoffs, so there is no need to run them in sequence.

Because the cutoff lands on a bucket edge, a sweep can free up to one bucket
more than strictly needed: a low-water margin of ten minutes of last-use
activity, finer than the once-a-day resolution `relatime` gives the access
times feeding it.

**Walk 2 (`sweepBelow`)** deletes what falls under the cutoff, in batches of
65,536. Each batch is de-advertised from `/_index` (`Index.RemoveKeys`) BEFORE
its files are unlinked: advertised-but-deleted is a forced miss on an indexed
key (the `miss_advertised_unservable` signature), while present-but-unadvertised
is at worst a redundant re-upload. It re-reads each object's last-use time, so
anything read between the two walks is spared, and `evictOne` re-stats before
unlinking so an object overwritten since the scan is not deleted.

Peak sweep memory is therefore a batch plus the histogram, not the cache.

## The schedule lives in the data_dir

`interval` defaults to 24h, and the end of each sweep is stamped into
`.last_sweep` in the data_dir.

On startup the server reads that marker and:

- **no marker, or a sweep at least an interval old** → sweep now, after a
  jittered 1–5 minute delay that spreads replicas restarting together;
- **a more recent sweep** → wait out the remainder of the interval.

Both halves matter. A schedule that restarted on every boot meant a deployment
that rolls more often than the interval — which is the production model — never
swept at all. A schedule that swept on every boot meant walking the whole disk
on every rolling update.

The `s3_cache_bytes` gauge is refreshed every 15 minutes on its own cadence, so
it is not stale by up to a whole interval.

## Configuration

See the README for the config reference. In short: `eviction.max_bytes`
(default 50 GiB, or the `CACHE_MAX_BYTES` env var) is the LRU bound,
`eviction.max_age` (default off) is the optional TTL, `eviction.interval`
(default 24h) is the sweep cadence, and setting both limits to 0 disables
eviction — which the server warns about, because the cache then grows until the
disk fills.
