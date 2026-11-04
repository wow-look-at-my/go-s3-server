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
	// maxSkipReportBytes bounds the body. A real report is a bounded number of bytes.
	maxSkipReportBytes = 4096
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
	PutFailed    uint64 `json:"put_failed"`
	PutRefused   uint64 `json:"put_refused"`
	PutAbandoned uint64 `json:"put_abandoned"`
}

var clientSkipsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_client_skips_total",
	Help: "Operations a client answered from its own index and never sent, by op (get, put) and reason.",
}, []string{"op", "reason"})

// clientUploadsLostTotal counts the uploads a client reported it never
// completed: failed (sent and not confirmed), refused (a client guard),
// abandoned (never sent).
var clientUploadsLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_client_uploads_lost_total",
	Help: "Uploads a client reported it never completed, by reason (failed, refused, abandoned).",
}, []string{"reason"})

func (r skipReport) gets() uint64 {
	return r.GetNotInIndex + r.GetEmptyIndex + r.GetBatchBackoff + r.GetKnownMiss
}

func (r skipReport) valid() bool {
	for _, v := range []uint64{r.GetNotInIndex, r.GetEmptyIndex, r.GetBatchBackoff, r.GetKnownMiss, r.PutKnown, r.PutFailed, r.PutRefused, r.PutAbandoned} {
		if v >= maxSkipReportCount {
			return false
		}
	}
	return true
}

// handleSkipReport counts a client's skipped gets as misses and its skipped
// puts as writes the cache already held.
func handleSkipReport(w http.ResponseWriter, r *http.Request) {
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
	clientUploadsLostTotal.WithLabelValues("refused").Add(float64(rep.PutRefused))
	clientUploadsLostTotal.WithLabelValues("abandoned").Add(float64(rep.PutAbandoned))
	noteProjectMiss(provenanceOf(r), int(rep.gets()))
	auditFromContext(r.Context()).note("skips get=%d put=%d lost_uploads=%d", rep.gets(), rep.PutKnown, rep.PutFailed+rep.PutRefused+rep.PutAbandoned)
	w.WriteHeader(http.StatusNoContent)
}
