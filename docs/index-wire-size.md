# What shrinks /_index, and what cannot
The index is one sorted list of action-ID hashes, framed by a header and a trailing digest (`index.go`). A client fetches it once per cold start and then asks it one question: may the server hold this key? The answer decides whether a lookup goes to the server at all.

## The frame

A GBCI blob is a 24-byte header, a body, and a SHA-256 trailer over header and body. The header holds the magic `GBCI` in `blob[0:4]`, the form's version in `blob[4]`, the prefix width in `blob[5]` (bytes for v1, bits for v2) and the Rice parameter in `blob[6]`. This also covers a body digest prefix in `blob[8:16]` and the entry count in `blob[16:24]`. The ETag is the hex trailer. The server keeps one sorted list of whole 32-byte action hashes (`index.go`) and serializes each form from it (`marshalForm` in `indexdelta.go`). It holds no second copy of the hashes to do that.

Each form has its own cached blob, ETag and staleness (`forms` in `index.go`), and each is serialized at most once per blob interval.

A v1 body is sorted and deduplicated fixed-size entries. Whole hashes, or, for a request that asked `hash=8`, the leading several bytes of each, with neighbours that share those bytes written once. A v2 body is the Rice-coded gaps between the sorted prefixes, padded to a byte.

A request that names no form gets v1 with whole hashes, byte for byte. The go toolchain vendors a `cacheclient` that reads that form only and never sends a parameter. The form is part of the URL, so a caching proxy stores the answers apart and needs no `Vary`. The current `cacheclient` parses any form by its header (`parseIndexBlob`), so an older server's v1 answer and an older client's disk copy both load. It holds the index as 8-byte keys (`keySet` in `cacheclient/indexkey.go`). Its disk copy is the answer as it arrived: a copy in another form does not match the asked-for form's ETag.

The server sends that form and the copy is replaced. `GET /_index` serves it in multiple forms. The request's `v` field names the one the client wants. The blob's own version byte says which it got, and `cacheclient` parses any of them.

| request | form | per key |
|---|---|---|
| no field | GBCI v1, whole hashes | 32 bytes |
| `hash=8` | GBCI v1, the first eight bytes of each hash | 8 bytes |
| `v=2` | GBCI v2, Rice-coded gaps between `index_hash_bits`-bit prefixes | about `index_hash_bits - log2(keys) + 2` bits |

`cacheclient` asks for `v=2&hash=8`, so a server of any age answers the smallest form it has.

## Measured, at a million keys

`TestIndexV2SizeAtAMillionKeys` in `cacheclient/indexv2_test.go` encodes the sha256 prefixes of a million counters and holds the blob to these lengths. `TestIndexV2RiceParameterIsNearOptimal` holds the Rice parameter to within a percent of the best one for that input.

| form | bytes | bits per key |
|---|---|---|
| v1, whole hashes | 32,000,056 | 256 |
| v1, `hash=8` | 8,000,056 | 64 |
| v2, 64 bits | 5,703,495 | 45.6 |
| v2, 48 bits | 3,703,495 | 29.6 |
| v2, 40 bits (the default) | 2,703,495 | 21.6 |

Each bit of width costs a million keys another 125,000 bytes, and each doubling of the key count saves a bit per key. The dashboard's index tile reports the v2 size of the live index, counted without serializing (`Index.WireSize`).

## Why the gaps compress when the hashes cannot

An action ID is a SHA-256: uniform, and incompressible on its own. gzip on the sorted v1 body saves under a percent. A SORTED list of n uniform w-bit values is different: the gap between neighbours is about `2^w / n`. A gap needs about `w - log2(n)` bits where the value needed `w`. The theoretical floor for n sorted distinct w-bit values is `n * (w - log2(n) + 1.44)` bits. Rice coding, with the high part of each gap in unary and the low k bits as they are, lands. Those lands are within a bit of that floor and decodes in one pass (`cacheclient/indexv2.go`).

Front-coding and delta-coding the full 32-byte hashes are the same arithmetic at `w = 256`. A 3% saving at a million keys, which is why the prefix width matters more than the coder.

## What a shorter prefix costs

The client consults the index in one direction. An absent prefix, with a fresh index, is a miss and skips its probe (`web.go`, the `indexAuthoritative` branch). A present prefix is then fetched, and that fetch is what answers. So a prefix that is present for ANOTHER key costs one batch round trip that answers not found. There are no false negatives: a key that is present keeps its prefix.

The client's claims go through the same keys. A Put claims the key's prefix, a Put whose prefix is already held is skipped as stored. A confirmed absence drops the prefix. A shared prefix therefore costs a probe that comes back empty or an upload skipped for a key the remote does hold. It never answers a fetch with the incorrect object. This is because a fetch is by the full key and the body is verified against its output ID.

The PUT path is safe for the same reason. A client skips uploading a key its set holds, but every `go` command looks a key up before it stores it. The not-found answer drops the prefix from the set (`reclaimAbsent`). The upload goes ahead.

The chance a lookup collides is about `keys / 2^index_hash_bits`: at many bits and a lower count.4 million keys, one lookup in 460,000. The config field `index_hash_bits` moves the width between 32 and 64. The server restarts with the new width and every client's next full fetch takes it. The look-ahead have-filter is unaffected: the client's held set is kept at the full 64 bits.

## The delta is the other half

The server keeps an add log of the hashes stored since its epoch began (`indexdelta.go`). Every full answer names its position in `X-Index-Epoch` and `X-Index-Seq`. A client holding a copy sends it back and takes only the keys stored since, in the same form, marked `X-Index-Delta: 1`. A removal, a rebuild or a full log starts a new epoch. A position from an old one is answered `410`, after which the client fetches the whole index. The forms above size the cold fetch.

The delta keeps the warm one small whatever the width.