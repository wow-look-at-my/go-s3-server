# The access log

The access log has a normal shape and a verbose shape. `log_mode` in the config file picks one, and `--log-mode` overrides it. The default is `normal`.

Everything else the server says is unaffected. A warning, an eviction sweep, a self-heal repair and a startup line print in both modes.

## normal

One line per SECOND in which the cache moved an object. A second with no traffic prints nothing.

```
2026/09/06 20:53:54 cache 1s: put=0 get=58 batched=72% compressed=3.4KiB/s uncompressed=13KiB/s ratio=26% projects=github.com/wow-look-at-my/go-s3-server, github.com/wow-look-at-my/scratch_ui
```

| Field | Meaning |
| --- | --- |
| `put` | objects stored that second, counting each member of a batch upload |
| `get` | objects served that second, counting each key streamed in a batch |
| `batched` | share of those objects that moved through `/_batch/get` or `/_batch/put` |
| `compressed` | bytes that crossed the wire, per second. Bodies are already lz4 from the client |
| `uncompressed` | the bytes those objects hold once decompressed, per second |
| `ratio` | compressed over uncompressed. Smaller is better compression |
| `sized` | present only when some objects declared no size. See below |
| `projects` | the modules the traffic belonged to, sorted, capped, then `+N more` |

The rates are per second by construction, because the line IS one second.

`uncompressed` and `ratio` cover only the objects whose client declared a `body-size`. When some object did not, `sized=N/total` says how many of that second's objects the two fields cover. Without that field the line reads as uncompressed being smaller than compressed, which no compressor does. The go-toolchain client always sends `body-size`, so `sized` on a real deployment means something else is writing to the cache.

A project is the `module` metadata the client sends. An object with no module falls back to the leading segments of its import path. That prefix is the closest thing to a project a package path carries.

## verbose

One line per request, and only one.

```
2026/09/06 20:53:56 req method=POST path=/cache/_batch/get client_ip=127.0.0.1 user=u user_agent="curl/8.5.0" status=200 bytes=50176 duration_ms=1 batch_get requested=12 found=12 prefetched=30 suppressed=0 streamed=42
```

A handler with something to add attaches it to that same line. The batch endpoints used to print their own summary line as well, so one batch request appeared twice under two spellings. Anything a handler wants said now rides the request it belongs to.

Use verbose to follow one client, or to see the key counts of a specific batch. A CI fleet issues thousands of requests a second, so leave it off under load. The per-request lines bury the very thing being looked for.

## Why normal is the default

A per-request log is unreadable exactly when it is most needed. During a CI storm the questions are how much traffic there is, whether batching is working, whether compression is paying, and which projects are moving. The per-second line answers all of those in one row. The row count then matches the wall clock, rather than the request count.
