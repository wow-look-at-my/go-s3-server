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

## What the pool is worth

`BenchmarkBuildShape` walks a 12-level graph, 4 keys wide, against a real HTTP server. Three shapes. `none` is the critical path alone, with nothing fetched ahead of it. `blocking` is the old wire shape, where 32 speculative bodies ride the request the build is waiting on. `lookahead` is what ships: the pool fetches the same window off the critical path, into the local tier the build reads next.

Every arm installs that tier. A run without one measures a client whose look-ahead is switched OFF, because `expand` returns at once when `OnBatchEntries` is nil. The benchmark used to omit it, so its old numbers described a shape nothing ships.

| | wall | bytes | allocations |
|---|---|---|---|
| loopback, none | 23.96 ms | 4.2 MB | 6,592 |
| loopback, blocking | 26.51 ms | 5.0 MB | 17,023 |
| loopback, lookahead | 5.01 ms | 1.2 MB | 5,410 |
| 5 ms RTT, none | 152.88 ms | 4.1 MB | 6,580 |
| 5 ms RTT, blocking | 147.04 ms | 5.0 MB | 17,406 |
| 5 ms RTT, lookahead | 36.13 ms | 1.2 MB | 5,379 |

Best of three runs each. The three runs of an arm never overlap another arm's.

The pool is worth 4.8x on loopback and 4.2x on the link, against fetching nothing ahead. It is worth 5.3x and 4.1x against the old shape. It also moves the least memory of the three. A body the build goes on to read is not waste.

The old shape is the row that explains the complaint the pool was built for. On loopback it is SLOWER than fetching nothing ahead at all. Its speculative bodies cost a real megabyte and buy the critical path nothing. The request they ride is the one the build is already blocked on. On the link its round trips hide that cost and it draws level. Neither is a cache worth having.

## The wire codec

The codec is zstd. It replaced lz4 because this cache's constraint is bandwidth rather than compression speed. A build waiting on a link cares how many bytes cross it. zstd carries far fewer of them for a comparable cost. The compression itself now runs on the prep pool, off the goroutine that just finished a compile.

A stored object names its codec in its own first four bytes. So a store holding both is read correctly without consulting metadata, and without a migration. The lz4 reader stays for as long as lz4 objects do.

The server reads that frame magic too, rather than the `compression` metadata. The two can disagree. Metadata lives in an xattr, and a `data_dir` copy that does not preserve xattrs loses it. That is the same failure `selfheal.go` exists to repair. The bytes cannot disagree with themselves.

One caller must not accept an unrecognized frame. `reconstructOutputID` hashes a decompressed body to rebuild a lost content address. A body that never decompressed hashes as it stands. That mints a confident wrong answer, and wedges the key for good. So `decompressingReader` reports which codec it recognized. That caller refuses an empty one.

`GO_TOOLCHAIN_CACHE_ZSTD_LEVEL` buys a smaller wire for more CPU.

## The write path holds a path, not a body

`Put` takes bytes. A caller holding a file must read the whole thing first. cmd/go's `offer` did that on the goroutine that had just finished a compile. The body then waited in the prep queue for a worker. That queue is four times the worker count deep. So peak resident uncompressed bodies was four times what the workers were compressing. On 32 cores that is 128 of them, which is the axis the Windows CI ran out of memory along.

`PutFile` takes a path. The claim stays on the caller's goroutine, because claiming is what stops two callers uploading one object, and it is a map probe. The read moves to `prepare`, which already needed the whole body for the build-id guard, the module-index guard, zstd and the metadata.

The caller owes the file's lifetime. A file that is gone by the time a worker opens it drops the upload and the claim together. A kept claim leaves the key advertised to this process and stored by nobody.

cmd/go satisfies the lifetime already. `SharedCache.Close` joins the backend's `Close` before the `DiskCache`'s. That drains the prep pool and the coalescer before anything trims.

## The rest of the read path

Two things a reader did per hit cost more than they had to.

The body was decompressed through `io.ReadAll`. That function cannot know the answer, so it grows a buffer by repeated reallocation. The uploader records the uncompressed length as `body-size`, and every read path carries it back. `DecompressSized` therefore allocates once. A declared size past `maxPresizedBody` is ignored. The value comes off the wire. It must not turn one response into an out-of-memory kill.

The index was built as one prefixed hex string per key. A key string is the same 32 bytes written as 64 hex characters behind a fixed prefix. A string set therefore costs about three times the memory. It also charges a hex encode and an allocation per entry. A production index is over 750,000 keys, and that work all lands at startup before the build does anything. The client indexes by the raw hash instead.

## Provenance

Every request carries who is asking. The headers are `X-Cache-Module`, `X-Cache-Toolchain`, `X-Cache-Target`, `X-Cache-Client`, and `X-Cache-Kind`. The last one is `critical` or `look-ahead`. A key says nothing about who wanted it. A server that cannot separate a waiting build from a reading-ahead one reports the two identically.

The server names those headers itself rather than importing the client for them. It reads them the way it reads `Content-Type`: as a wire contract any client version may or may not honor. Importing the client puts that whole package in the server's dependency graph, and therefore in the server's coverage, for five strings. `TestProvenanceHeadersMatchTheClient` pins the two spellings together, in a test, where the import costs nothing.
