package cacheclient

// WebSummary is a point-in-time snapshot of the web tier's diagnostic
// counters — the same numbers the daemon prints as its "web summary" stderr
// line — plus the startup index state. It is serialized into the build
// profile (build/profile.json) so CI can assert on the poison tripwires
// (miss_checksum / miss_buildid / miss_modindex — a served object failing an
// integrity gate) and the dead-remote signature (a populated advertised index
// yielding no hits).
type WebSummary struct {
	Hits uint32 `json:"hits"`
	Puts uint32 `json:"puts"`

	// GET miss reasons; miss_checksum/miss_buildid/miss_modindex are the poison tripwires (a served object a guard refused).
	MissNotInIndex  uint32 `json:"miss_not_in_index"`
	MissHTTP404     uint32 `json:"miss_http_404"`
	MissHTTPError   uint32 `json:"miss_http_error"`
	MissNoOutputID  uint32 `json:"miss_no_outputid"`
	MissReadBody    uint32 `json:"miss_read_body"`
	MissDecompress  uint32 `json:"miss_decompress"`
	MissChecksum    uint32 `json:"miss_checksum"`
	MissBuildID     uint32 `json:"miss_buildid"`
	MissModuleIndex uint32 `json:"miss_modindex"`
	MissNetwork     uint32 `json:"miss_network"`

	// No-round-trip skip breakdowns (subsets of miss_not_in_index).
	SkippedEmptyIndex   uint32 `json:"skipped_empty_index"`
	SkippedNotInIndex   uint32 `json:"skipped_not_in_index"`
	SkippedBatchBackoff uint32 `json:"skipped_batch_backoff"`
	Reclaimed404        uint32 `json:"reclaimed_404"`

	// PUT non-upload outcomes. put_refused_modindex reads empty normally (handlePut refuses earlier); a count is a local gap.
	PutSkippedKnown    uint32 `json:"put_skipped_known"`
	PutRefusedModIndex uint32 `json:"put_refused_modindex"`
	PutRefusedBuildID  uint32 `json:"put_refused_buildid"`

	// Startup index state: advertised key count, and whether it came from a fresh fetch this run.
	IndexKeys          int  `json:"index_keys"`
	IndexAuthoritative bool `json:"index_authoritative"`
}

// MissTotal sums the GET miss reasons. The skipped_* counters are breakdowns
// of miss_not_in_index (each such Get already incremented it), so they are
// not added — that would double-count.
func (ws WebSummary) MissTotal() uint32 {
	return ws.MissNotInIndex + ws.MissHTTP404 + ws.MissHTTPError +
		ws.MissNoOutputID + ws.MissReadBody + ws.MissDecompress +
		ws.MissChecksum + ws.MissBuildID + ws.MissModuleIndex + ws.MissNetwork
}

// SummarySnapshot captures the backend's diagnostic counters. Values are read
// individually from live atomics, so a snapshot taken while operations are in
// flight is approximate; taken after Close it is exact.
func (b *WebBackend) SummarySnapshot() WebSummary {
	return WebSummary{
		Hits:                b.Stats.Hits.Load(),
		Puts:                b.Stats.Puts.Load(),
		MissNotInIndex:      b.MissNotInIndex.Load(),
		MissHTTP404:         b.MissHTTP404.Load(),
		MissHTTPError:       b.MissHTTPError.Load(),
		MissNoOutputID:      b.MissNoOutputID.Load(),
		MissReadBody:        b.MissReadBody.Load(),
		MissDecompress:      b.MissDecompress.Load(),
		MissChecksum:        b.MissChecksum.Load(),
		MissBuildID:         b.MissBuildID.Load(),
		MissModuleIndex:     b.MissModuleIndex.Load(),
		MissNetwork:         b.MissNetwork.Load(),
		SkippedEmptyIndex:   b.SkippedEmptyIndex.Load(),
		SkippedNotInIndex:   b.SkippedNotInIndex.Load(),
		SkippedBatchBackoff: b.SkippedBatchBackoff.Load(),
		Reclaimed404:        b.Reclaimed404.Load(),
		PutSkippedKnown:     b.PutSkippedKnown.Load(),
		PutRefusedModIndex:  b.PutRefusedModIndex.Load(),
		PutRefusedBuildID:   b.PutRefusedBuildID.Load(),
		IndexKeys:           b.indexKeysAtStart,
		IndexAuthoritative:  b.indexAuthoritative,
	}
}
