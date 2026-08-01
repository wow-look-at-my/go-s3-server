# Where the server's CPU goes

The cache is write-bursty and read-heavy: a CI matrix uploads thousands of
objects in a burst and then every later build asks for its keys back, 128 at a
time, through `/_batch/get`. The server compresses nothing and hashes nothing
on the hot path, so its CPU is (a) syscalls per key and (b) work done per byte
to answer questions that do not need the bytes.

Both were larger than the actual I/O. Measured on this repo's benchmarks
(`hotpath_bench_test.go`, `go-toolchain bench run`):

| benchmark | before | after |
|---|---|---|
| `ModuleIndexPeek/put` (256 KiB object) | 77.0 us | 0.18 us |
| `ModuleIndexPeek/read` | 76.2 us | 0.32 us |
| `StatMetadata` (one key's metadata) | 42.1 us | 3.8 us |
| `BatchGetWarm` (128 keys, warm) | 21.0 ms, 8.5 MB, 24446 allocs | 5.5 ms, 4.8 MB, 5612 allocs |
| `GetObjectWarmLz4/nomemo` | 59.2 us | 25.0 us |

## 1. The module-index guard decoded a block to read ten bytes

Every PUT, and every cold read, decompressed the body's first lz4 block to
recover the `go index v` magic. It now walks the frame header and reads the
first literal run instead. See `docs/module-index-guard.md`.

Side effect: the known-clean memo (`cleanmemo.go`) now saves almost nothing --
the probe it skips costs 300ns. It stays because it also skips a file open.

## 2. Metadata was re-read from the kernel on every touch

User metadata lives in xattrs, and reading it is one `listxattr` plus one
`getxattr` per attribute -- around a dozen syscalls for a typical entry
(outputid, compression, object-type, pkg, src, module, go-version, target,
toolchain-version, created). At ~1.9us per syscall that is the 42us above, and
a 128-key batch paid it 128 times. Three separate fixes:

- **`metacache.go` -- cache it.** Keyed by storage key, tagged with the mtime
  and size it was read under, and validated against the stat every caller
  already performs. A new body arrives as a fresh inode renamed over the path,
  so an overwrite can never be served with the previous body's metadata: the
  stat no longer matches and the entry is a miss. The one mutation that leaves
  mtime alone -- an xattr stamped onto a live inode by the outputid self-heal
  -- drops the entry explicitly (`forgetMeta`), as do delete and eviction.
  Bounded at 64k entries; past the bound it clears wholesale.
  `s3_meta_cache_hits_total` / `s3_meta_cache_misses_total` expose the ratio.
- **The batch path read it twice per key.** Phase 1 stats every key to build
  the manifest; phase 2 called `Open`, which read the xattrs again to build an
  `ObjectMeta` nobody consumed. Phase 2 now uses `Storage.OpenBody` (open +
  size + last-access, no metadata read).
- **Halve the syscalls on a miss.** `listxattr` and `getxattr` were each
  issued twice per call -- once to size the buffer, once to fill it. They now
  read into a stack buffer and only fall back to the probe on `ERANGE`.

## 3. Audit attributes were being read as user metadata

`user.s3audit.` begins with `user.s3.`, so the namespace test matched audit
attributes too. Every metadata read fetched them (five more `getxattr` calls),
and every GET emitted them as `X-Cache-Meta-Audit.*` response headers --
handing the uploader's identity and client IP to anyone who could fetch the
object. `isUserMetaAttr` now excludes the audit namespace;
`TestAuditXattrsAreNotServedAsMetadata` pins both halves.

## 4. Copy buffers, one per batch member

`io.CopyN` allocates a fresh 32 KiB buffer per call and the batch response is
one call per member, so a 128-key batch churned ~4 MiB of pure scratch --
GC time on the busiest path. `writeTarEntry` now copies through a pooled
buffer. (The tar writer implements neither `ReaderFrom` nor `WriterTo`, so an
explicit buffer is what actually gets used.)

## What is left

`BatchGetWarm` is now dominated by the body copy itself -- real I/O for real
bytes. The remaining per-key overhead is the stat, the open, and four
Prometheus label lookups; none of it is worth the churn of removing.

The other thing that would show up as server CPU is not the server's: a ZFS
dataset with `compression` on recompresses every stored body, which the client
already lz4-compressed. `compression.go` warns about that once at startup.
