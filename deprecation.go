package main

import (
	"log"
	"net/http"
	"sync"
)

// This file centralizes the backward-compatibility shims for the
// S3-compatible protocol.

const (
	// nativeMetaPrefix is the current, non-S3 request/response header prefix for user object metadata.
	nativeMetaPrefix = "x-cache-meta-"
	// legacyMetaPrefix is the deprecated S3-style metadata header prefix.
	legacyMetaPrefix = "x-amz-meta-"
)

// featureAmzMeta labels deprecated requests that carried S3-style X-Amz-Meta-* metadata headers.
const featureAmzMeta = "amz_meta_header"

var s3MetaDeprecationOnce sync.Once

// noteDeprecatedS3Meta records that a client used the deprecated S3-style
// X-Amz-Meta-* request headers. It logs a single prominent warning the
// earliest time (a CI run issues thousands of PUTs, so per-request logging
// would flood the log) and always increments the counter, so the ongoing
// volume stays visible in metrics.
func noteDeprecatedS3Meta(r *http.Request) {
	deprecatedRequestsTotal.WithLabelValues(featureAmzMeta).Inc()
	s3MetaDeprecationOnce.Do(func() {
		log.Printf("DEPRECATION: client sent S3-style X-Amz-Meta-* metadata headers (client_ip=%s user_agent=%q); these are deprecated in favor of X-Cache-Meta-* and will be removed in a future release -- upgrade go-toolchain. Further occurrences are counted in the s3_deprecated_requests_total metric.",
			clientIP(r), r.UserAgent())
	})
}
