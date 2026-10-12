# The /_index wire format, and what shrinks it

## The frame

A GBCI blob is a 24-byte header, a sorted and deduplicated body of fixed-size entries, and a SHA-256 trailer over header and body. The header holds the magic `GBCI` in `blob[0:4]`, the version in `blob[4]`, the entry size in `blob[5]`, a body digest prefix. That prefix is in `blob[8:16]` and the entry count in `blob[16:24]`. The ETag is the hex trailer. The server keeps one sorted list of whole 32-byte action hashes (`index.go`) and serializes each version from it (`marshalGBCI` in `indexdelta.go`).

| version | `blob[5]` | entry | how a client asks |
|---|---|---|---|
| 1 | 32 | the whole action hash | no `v` parameter |
| 2 | 8 | the leading 8 bytes of the action hash | `GET /<bucket>/_index?v=2` |

A request without `v=2` gets version 1, byte for byte. The go toolchain vendors a `cacheclient` that reads version 1 only. It never sends the parameter. The version is part of the URL, so a caching proxy stores the answers apart and needs no `Vary`. Each version has its own cached blob, ETag and staleness (`servedBlob`), and each is serialized at most once per blob interval. Version 2 holds no second copy of the hashes: it reads the leading bytes of the same list.

Neighbouring hashes that share their leading bytes become one version 2 entry, so its body stays sorted and deduplicated.

The current `cacheclient` sends `v=2` on every index request, full or delta. It parses either version by the header (`parseIndexBlob`), so an older server's version 1 answer and a version 1 disk copy both load. It holds the index as 8-byte keys (`keySet` in `cacheclient/indexkey.go`). Its disk copy is the answer as it arrived. A version 1 copy revalidated with `v=2` does not match the version 2 ETag. The server sends version 2 and the copy is replaced.

## Why 8 bytes is enough

The client consults the index in one direction. An absent key, with an authoritative index, is a miss and skips its probe (`web.go`, the `indexAuthoritative` branch). A present key is then actually fetched, and that fetch is what answers. There are no false negatives: a stored key keeps its prefix.

The client's claims go through the same keys. A Put claims the key's prefix, a Put whose prefix is already held is skipped as stored. A confirmed absence drops the prefix. So a shared prefix costs one of two things: a probe that comes back empty, or an upload skipped. This is because a different key with the same prefix is stored. It never answers a fetch with the incorrect object.

This is because a fetch is by the full key and the body is verified against its output ID.

With n keys held, the chance a given hash shares its 8-byte prefix with any of them is about `n / 2^64`. At 3.5 million keys that is about 2e-13 per lookup. The version 2 blob at that size is 28 MB, where version 1 is 112 MB, and the client holds several bytes per key in memory where it held 32.

## A delta against the client's position

The server keeps an add log of the hashes stored since its epoch began (`indexdelta.go`). Every full answer names its position in `X-Index-Epoch` and `X-Index-Seq`. A client with a young disk copy asks `GET /<bucket>/_index?v=2&epoch=E&seq=S`. It gets a blob of only the hashes added since, marked `X-Index-Delta: 1`, in the version it asked for. A removal, a rebuild or a full log starts a new epoch. A position from an old epoch is answered `410`, after which the client fetches the whole index.

## Compressing the hashes does not work

An action ID is a SHA-256. Sorted, uniformly distributed, incompressible by construction: N random 256-bit values need at least `N * (256 - log2(N))` bits, so the whole theoretical prize at a million keys is 7.8%. Measured on random sorted hashes:

| keys | front-coded | gzip |
|---|---|---|
| 10,000 | 99.9% | 100.0% |
| 100,000 | 98.5% | 99.8% |
| 1,000,000 | 97.0% | 99.5% |

Front-coding a shared prefix wins 3% at a million keys and nothing at ten thousand. This is because the share of the prefix multiple neighbours is about `log2(N)` bits. Delta-coding consecutive values is the same arithmetic and lands in the same place. Neither is worth a wire-format change. Truncation and the delta are what shrink the index.
