package main

import (
	"log"
	"net/http"
	"sync"
)

// This file centralizes the backward-compatibility shims for the old
// S3-compatible protocol. The cache protocol is no longer S3-compatible: the
// go-toolchain client now speaks a native protocol (native error bodies and
// X-Cache-Meta-* metadata headers). The shims below keep not-yet-upgraded
// clients working, but every use of a deprecated S3 feature is surfaced --
// logged once and counted in the s3_deprecated_requests_total metric -- so the
// remaining S3 traffic is visible and the shims can be removed once it drops to
// zero (slated for the repository rename to go-toolchain-cache).

const (
	// nativeMetaPrefix is the current, non-S3 request/response header prefix
	// for user object metadata.
	nativeMetaPrefix = "x-cache-meta-"
	// legacyMetaPrefix is the deprecated S3-style metadata header prefix. It is
	// still accepted on PUT and still emitted on GET so older go-toolchain
	// clients keep functioning, but its use is flagged as deprecated.
	legacyMetaPrefix = "x-amz-meta-"
)

// featureAmzMeta labels deprecated requests that carried S3-style
// X-Amz-Meta-* metadata headers.
const featureAmzMeta = "amz_meta_header"

var s3MetaDeprecationOnce sync.Once

// noteDeprecatedS3Meta records that a client used the deprecated S3-style
// X-Amz-Meta-* request headers. It logs a single prominent warning the first
// time (a CI run issues thousands of PUTs, so per-request logging would flood
// the log) and always increments the counter, so the ongoing volume stays
// visible in metrics.
func noteDeprecatedS3Meta(r *http.Request) {
	deprecatedRequestsTotal.WithLabelValues(featureAmzMeta).Inc()
	s3MetaDeprecationOnce.Do(func() {
		log.Printf("DEPRECATION: client sent S3-style X-Amz-Meta-* metadata headers (client_ip=%s user_agent=%q); these are deprecated in favor of X-Cache-Meta-* and will be removed in a future release -- upgrade go-toolchain. Further occurrences are counted in the s3_deprecated_requests_total metric.",
			clientIP(r), r.UserAgent())
	})
}
