# Look-ahead, and why the critical path carries nothing else

## What a build can ask for

A build asks for one cache key at a time, in dependency order. It cannot name the next key until this one answers. A package's action ID is computed from its dependencies' output IDs. So the number of keys a build has outstanding is its own `-p`. No amount of batching on the client changes that. A four-way build offers four keys, waits a round trip, then offers four more.

That has two consequences the client is built around. The number of round trips is fixed by the graph's depth. On a link with a real round-trip time, that latency IS the cost of a cached build. The only way to spend fewer round trips is to fetch objects nobody has asked for yet.

## Where the speculative fetching runs

The server can name those objects. It records each one's modification time. The objects one build writes land next to each other in that order. So the window around a key this build just wanted is mostly keys this build wants next. `/_batch/get` returns them when the request sets `prefetch`.

That work used to ride the request the build was blocked on. It cost more than it saved. The numbers were not close. One toolchain build reported this:

    batch GET x257: 578 keys -> 1816 entries (1238 prefetched), 38115ms total

1238 of 1816 bodies were speculative. Every one of them queued ahead of the four bodies the compiler was stalled on, on one connection. Worse, nothing consumed them. `OnBatchEntries` was never set by the only consumer, so each entry was parsed, held, and dropped. The build paid the bandwidth and got nothing.

So look-ahead moved off that path.

- `sendBatch` asks for the requested keys and nothing else. Its response is as small as the build made it.
- `lookAhead` is a pool of goroutines sized to the machine rather than to the build's `-p`. It issues its own requests with `prefetch_only`. That flag returns the window around a set of anchor keys without their bodies, which the caller already holds. Nothing blocks on these requests.
- Each pool worker hands its own answer to `OnBatchEntries`. Verification and the local write therefore happen at the pool's width.

A hit seeds the pool with the keys that answered, deduplicated. A key that missed says nothing about where to look. A full queue drops the seed. Look-ahead is speculation. A build goroutine must never wait on it.

`GO_TOOLCHAIN_CACHE_LOOKAHEAD` sets the worker count.

## What it costs when it rides the blocking path

`BenchmarkBuildShape` walks a 12-level graph, 4 keys wide, against a real HTTP server. `carried=32` is the old shape: 32 speculative bodies attached to every blocking response. `carried=0` is what the critical path asks for now.

| | wall | bytes | allocations |
|---|---|---|---|
| loopback, old (lz4, carried=32) | 32.3 ms | 13.9 MB | 33,318 |
| loopback, new (zstd, carried=0) | 23.0 ms | 4.2 MB | 6,443 |
| 5 ms RTT, old (lz4, carried=32) | 161.2 ms | 7.1 MB | 32,546 |
| 5 ms RTT, new (zstd, carried=0) | 152.2 ms | 4.3 MB | 6,500 |

That is 29% off the wall time, 70% off the memory, and five times fewer allocations. Most of the remaining wall time is the round trips, which no client can remove. Most of what went was bodies nobody asked for. On a real link the byte column dominates: there the round trip is tens of milliseconds and the bandwidth is finite.

## The wire codec

The codec is zstd. It replaced lz4 because this cache's constraint is bandwidth rather than compression speed. A build waiting on a link cares how many bytes cross it. zstd carries far fewer of them for a comparable cost. The compression itself now runs on the prep pool, off the goroutine that just finished a compile.

A stored object names its codec in its own first four bytes. So a store holding both is read correctly without consulting metadata, and without a migration. The lz4 reader stays for as long as lz4 objects do.

The server reads that frame magic too, rather than the `compression` metadata. The two can disagree. Metadata lives in an xattr, and a `data_dir` copy that does not preserve xattrs loses it. That is the same failure `selfheal.go` exists to repair. The bytes cannot disagree with themselves.

One caller must not accept an unrecognized frame. `reconstructOutputID` hashes a decompressed body to rebuild a lost content address. A body that never decompressed hashes as it stands. That mints a confident wrong answer, and wedges the key for good. So `decompressingReader` reports which codec it recognized. That caller refuses an empty one.

`GO_TOOLCHAIN_CACHE_ZSTD_LEVEL` buys a smaller wire for more CPU.

## The rest of the read path

Two things a reader did per hit cost more than they had to.

The body was decompressed through `io.ReadAll`. That function cannot know the answer, so it grows a buffer by repeated reallocation. The uploader records the uncompressed length as `body-size`, and every read path carries it back. `DecompressSized` therefore allocates once. A declared size past `maxPresizedBody` is ignored. The value comes off the wire. It must not turn one response into an out-of-memory kill.

The index was built as one prefixed hex string per key. A key string is the same 32 bytes written as 64 hex characters behind a fixed prefix. A string set therefore costs about three times the memory. It also charges a hex encode and an allocation per entry. A production index is over 750,000 keys, and that work all lands at startup before the build does anything. The client indexes by the raw hash instead.

## Provenance

Every request carries who is asking. The headers are `X-Cache-Module`, `X-Cache-Toolchain`, `X-Cache-Target`, `X-Cache-Client`, and `X-Cache-Kind`. The last one is `critical` or `look-ahead`. A key says nothing about who wanted it. A server that cannot separate a waiting build from a reading-ahead one reports the two identically.

The server names those headers itself rather than importing the client for them. It reads them the way it reads `Content-Type`: as a wire contract any client version may or may not honor. Importing the client puts that whole package in the server's dependency graph, and therefore in the server's coverage, for five strings. `TestProvenanceHeadersMatchTheClient` pins the two spellings together, in a test, where the import costs nothing.
