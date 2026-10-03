# What can shrink /_index, and what cannot

The index is one sorted array of 32-byte action-ID hashes, framed by a
header and a trailing digest (`index.go`, `Index.Blob`). At a million
keys it is 32 MB, and every cold client fetches it.

## Compressing the hashes does not work

An action ID is a SHA-256. Sorted, uniformly distributed, incompressible
by construction: N random 256-bit values need at least
`N * (256 - log2(N))` bits, so the whole theoretical prize at a million
keys is 7.8%. Measured on random sorted hashes:

| keys | front-coded | gzip |
|---|---|---|
| 10,000 | 99.9% | 100.0% |
| 100,000 | 98.5% | 99.8% |
| 1,000,000 | 97.0% | 99.5% |

Front-coding a shared prefix wins 3% at a million keys and nothing at
ten thousand, because the prefix two neighbours share is about
`log2(N)` bits. Delta-coding consecutive values is the same arithmetic
and lands in the same place. Neither is worth a wire-format change.

## Truncation does work, and the index can afford it

The client consults the index in ONE direction. An absent key, with a
fresh index, is a miss and skips its probe (`web.go`, the
`indexAuthoritative` branch). A present key is then actually fetched,
and that fetch is what answers.

So a truncated hash can only ever cost a probe that comes back empty.
It cannot skip an upload, and it cannot answer a fetch wrongly. There
are no false negatives either: a key that is present keeps its prefix.

Eight bytes per hash cuts the index to a quarter. With a million keys
the chance any one lookup collides is about `1e6 / 2^64`, roughly
5e-14, against a saving of 24 MB on every cold fetch.

The header already carries the per-entry size in `blob[5]`, so the frame
does not change shape. What it costs is a version bump and a client that
reads the size instead of assuming it: `cacheclient` hard-codes
`hashSize = 32` and types `actionHash` as `[32]byte`, and that client is
vendored into the fork's `cmd/go`, so the two move together.

## A delta against the client's ETag also works

The client sends `If-None-Match` and takes a 304. Answering "here is
what changed since that ETag" instead of the whole array is lossless and
shrinks the steady state far below anything the format itself can do.
It costs the server a retained history: the generation field is
content-derived, so two blobs have no order between them and there is
nothing to diff against today.

## Where this leaves it

Truncation is the cheap one and the frame already has the field for it.
The ETag delta is the larger win and the larger change. Front-coding,
which is what this was first written down as, is measurably not worth
doing.
