// A client answers many reads from its own copy of /_index and never sends them.

package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A client answers many reads from its own copy of /_index and never sends
// them.

const (
	skipReportKey = "_skips"
	// maxSkipReportBytes bounds the body. The counts and a full sample fit well inside it.
	maxSkipReportBytes = 16 << 10
	// maxSkipReportCount bounds a single field. The client counts in uint32.
	maxSkipReportCount = 1 << 32
)

// skipReport is the wire body. cacheclient.SkipReport is the other side.
type skipReport struct {
	GetNotInIndex   uint64 `json:"get_not_in_index"`
	GetEmptyIndex   uint64 `json:"get_empty_index"`
	GetBatchBackoff uint64 `json:"get_batch_backoff"`
	GetKnownMiss    uint64 `json:"get_known_miss"`
	PutKnown        uint64 `json:"put_known"`
	// Uploads that never reached the store.
	PutFailed          uint64 `json:"put_failed"`
	PutRefusedBuildID  uint64 `json:"put_refused_buildid"`
	PutRefusedModIndex uint64 `json:"put_refused_modindex"`
	PutAbandoned       uint64 `json:"put_abandoned"`
	// PutRefused is the count a client of the first report shape sends for both refusals.
	PutRefused uint64 `json:"put_refused"`
	// SkippedKeys is a sample of the keys behind GetNotInIndex, as hex action ids.
	SkippedKeys []string `json:"skipped_keys"`
}

// maxSkipSample bounds the sample one report carries. cacheclient sends at most this many.
const maxSkipSample = 64

// clientSkipSamplesTotal classifies the sampled keys: evicted (the sweeper
// removed it since the client fetched its index), advertised (the server
// holds it now, so the client's index copy was stale), unknown (never held,
// as far as the server remembers).
var clientSkipSamplesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_client_skip_samples_total",
	Help: "Keys clients sampled from their index skips, by what the server knows of them (evicted, advertised, unknown).",
}, []string{"verdict"})

var clientSkipsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_client_skips_total",
	Help: "Operations a client answered from its own index and never sent, by op (get, put) and reason.",
}, []string{"op", "reason"})

// clientUploadsLostTotal counts the uploads a client reported it never
// completed: failed (sent and not confirmed), refused_buildid (the body's
// build id disagreed with its key), refused_modindex (a Go module index),
// abandoned (never sent).
var clientUploadsLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_client_uploads_lost_total",
	Help: "Uploads a client reported it never completed, by reason (failed, refused_buildid, refused_modindex, abandoned).",
}, []string{"reason"})

func (r skipReport) gets() uint64 {
	return r.GetNotInIndex + r.GetEmptyIndex + r.GetBatchBackoff + r.GetKnownMiss
}

func (r skipReport) valid() bool {
	for _, v := range []uint64{r.GetNotInIndex, r.GetEmptyIndex, r.GetBatchBackoff, r.GetKnownMiss, r.PutKnown, r.PutFailed, r.PutRefusedBuildID, r.PutRefusedModIndex, r.PutAbandoned, r.PutRefused} {
		if v >= maxSkipReportCount {
			return false
		}
	}
	if len(r.SkippedKeys) > maxSkipSample {
		return false
	}
	for _, k := range r.SkippedKeys {
		if _, ok := extractActionHash(gbciKeyPrefix + k); !ok {
			return false
		}
	}
	return true
}

// classifySkippedKeys says what the server knows of each sampled key.
func classifySkippedKeys(storage *Storage, keys []string) {
	for _, k := range keys {
		verdict := "unknown"
		switch key := gbciKeyPrefix + k; absentKeyReason(storage, key) {
		case missAdvertised:
			verdict = "advertised"
		case missEvicted:
			verdict = "evicted"
		}
		clientSkipSamplesTotal.WithLabelValues(verdict).Inc()
	}
}

// handleSkipReport counts a client's skipped gets as misses and its skipped
// puts as writes the cache already held.
func handleSkipReport(w http.ResponseWriter, r *http.Request, storage *Storage) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSkipReportBytes))
	dec.DisallowUnknownFields()
	var rep skipReport
	if err := dec.Decode(&rep); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, 413, "too_large", "skip report too large")
			return
		}
		writeError(w, 400, "bad_skip_report", "skip report: "+err.Error())
		return
	}
	if !rep.valid() {
		writeError(w, 400, "bad_skip_report", "skip report: a count is out of range")
		return
	}
	clientSkipsTotal.WithLabelValues("get", "not_in_index").Add(float64(rep.GetNotInIndex))
	clientSkipsTotal.WithLabelValues("get", "empty_index").Add(float64(rep.GetEmptyIndex))
	clientSkipsTotal.WithLabelValues("get", "batch_backoff").Add(float64(rep.GetBatchBackoff))
	clientSkipsTotal.WithLabelValues("get", "known_miss").Add(float64(rep.GetKnownMiss))
	clientSkipsTotal.WithLabelValues("put", "known").Add(float64(rep.PutKnown))
	clientUploadsLostTotal.WithLabelValues("failed").Add(float64(rep.PutFailed))
	clientUploadsLostTotal.WithLabelValues("refused_buildid").Add(float64(rep.PutRefusedBuildID))
	clientUploadsLostTotal.WithLabelValues("refused_modindex").Add(float64(rep.PutRefusedModIndex))
	clientUploadsLostTotal.WithLabelValues("refused").Add(float64(rep.PutRefused))
	clientUploadsLostTotal.WithLabelValues("abandoned").Add(float64(rep.PutAbandoned))
	if storage != nil {
		classifySkippedKeys(storage, rep.SkippedKeys)
	}
	noteProjectMiss(provenanceOf(r), int(rep.gets()))
	auditFromContext(r.Context()).note("skips get=%d put=%d lost_uploads=%d sampled=%d", rep.gets(), rep.PutKnown,
		rep.PutFailed+rep.PutRefusedBuildID+rep.PutRefusedModIndex+rep.PutRefused+rep.PutAbandoned, len(rep.SkippedKeys))
	w.WriteHeader(http.StatusNoContent)
}
