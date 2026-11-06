// Why the cache missed: every miss the server knows of, sorted by its cause.

package main

import (
	"sort"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// evictedMemoKind is the label the evicted-key memo reports its size under.
const evictedMemoKind = "evicted-keys"

// evictedEntryBytes is what one remembered victim costs: the hash, the map bucket and the list element.
const evictedEntryBytes = 96

// newEvictedMemo remembers the action hashes the sweeper removed. A request
// for one of them is a capacity miss, not an unknown key. The memo is
// bounded, so a victim old enough to fall out of it reads as unknown again.
func newEvictedMemo(budget int64) *lruCache[cleanKey, struct{}] {
	return newLRUCache(budget,
		func(h cleanKey) uint32 {
			return uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16 | uint32(h[3])<<24
		},
		func(cleanKey, struct{}) int64 { return evictedEntryBytes })
}

// noteEvicted records that the sweeper removed key. Nil-safe for a
// directly-constructed Storage.
func (s *Storage) noteEvicted(key string) {
	h, ok := extractActionHash(key)
	if !ok || s.evicted == nil {
		return
	}
	s.evicted.Put(h, struct{}{})
}

// wasEvicted reports whether the sweeper removed the key recently enough for
// the memo to still hold it.
func (s *Storage) wasEvicted(key string) bool {
	h, ok := extractActionHash(key)
	if !ok || s.evicted == nil {
		return false
	}
	_, found := s.evicted.Get(h)
	return found
}

// forgetEvicted drops the memo entry when a key is stored again.
func (s *Storage) forgetEvicted(key string) {
	h, ok := extractActionHash(key)
	if !ok || s.evicted == nil {
		return
	}
	s.evicted.Forget(h)
}

// The reasons a key a batch asked for was left out of the answer. The single
// GET path records the same set under s3_get_requests_total with a miss_
// prefix.
const (
	missNotFound     = "not_found"
	missEvicted      = "evicted"
	missAdvertised   = "advertised_unservable"
	missModuleIndex  = "module_index_evicted"
	missPeekError    = "peek_error"
	missSelfHeal     = "selfheal_failed"
	missStoredDigest = "stored_digest"
	missStatError    = "stat_error"
)

// batchMissesTotal counts the requested keys a batch GET could not answer, by
// reason. A prefetch candidate that is dropped is not a miss and is not here.
var batchMissesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_batch_misses_total",
	Help: "Keys a /_batch/get asked for and did not get, by reason (not_found, evicted, advertised_unservable, module_index_evicted, peek_error, selfheal_failed, stored_digest, stat_error).",
}, []string{"reason"})

// absentKeyReason classifies a key the store does not hold. An advertised key
// is the index and store disagreeing. A remembered victim is a key the
// sweeper removed since the client fetched its index.
func absentKeyReason(storage *Storage, key string) string {
	if h, ok := extractActionHash(key); ok && storage.Index != nil && storage.Index.Contains(h) {
		return missAdvertised
	}
	if storage.wasEvicted(key) {
		return missEvicted
	}
	return missNotFound
}

// missSource is who decided the miss.
const (
	missSourceClient    = "client"    // the client answered from its own index copy and sent nothing
	missSourceServer    = "server"    // the server looked and had no body
	missSourceIntegrity = "integrity" // the server had a body it refused to serve
)

// missSeverity is what a reason says about the cache.
const (
	missSeverityExpected = "expected" // the cache is working; the key was never there
	missSeverityCapacity = "capacity" // the cache held the key and let it go
	missSeverityBug      = "bug"      // the cache should have served the key
)

// missReasonRow is one cause on the WHY MISSES panel: the part that moves
// between snapshots. The label and the prose are in the vocabulary.
type missReasonRow struct {
	Key      string  `json:"key"`
	Count    float64 `json:"count"`
	Share    float64 `json:"share"`
	Source   string  `json:"source"`
	Severity string  `json:"severity"`
}

// missReasonSpec is the fixed part of a row.
type missReasonSpec struct {
	key, label, source, severity, explain, action string
	count                                         func(stats map[string]metricValue) float64
}

// missReasonText is the vocabulary entry of a cause: what never changes
// between snapshots. The severity here is the one at rest. A row carries
// its own, because a sample can raise it.
type missReasonText struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Source   string `json:"source"`
	Severity string `json:"severity"`
	Explain  string `json:"explain"`
	Action   string `json:"action"`
}

// missReasonVocab is every cause's text, in display order.
func missReasonVocab() []missReasonText {
	out := make([]missReasonText, 0, len(missReasonSpecs))
	for _, spec := range missReasonSpecs {
		out = append(out, missReasonText{Key: spec.key, Label: spec.label, Source: spec.source, Severity: spec.severity, Explain: spec.explain, Action: spec.action})
	}
	return out
}

func missReasonSpecByKey(key string) missReasonSpec {
	for _, spec := range missReasonSpecs {
		if spec.key == key {
			return spec
		}
	}
	return missReasonSpec{}
}

// missSignal is a number that explains a cause without being one. A rising
// upload refusal count explains a rising never-stored count.
type missSignal struct {
	Key   string  `json:"key"`
	Count float64 `json:"count"`
	Alert bool    `json:"alert"`
	Note  string  `json:"note,omitempty"`
}

// missSignalText is the vocabulary entry of a signal.
type missSignalText struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Note  string `json:"note,omitempty"`
}

// missSignalVocab is every signal's label and note, in display order. The
// cache_fill note depends on the numbers, so its row carries it instead.
var missSignalVocab = []missSignalText{
	{Key: "cache_fill", Label: "cache size against its budget"},
	{Key: "uploads_failed", Label: "uploads the client sent and the store never confirmed",
		Note: "reported by the client at exit: a network error, a refused answer, or a per-object error. Each is a key that misses on every later build until something uploads it again"},
	{Key: "uploads_abandoned", Label: "uploads the client gave up without sending",
		Note: "reported by the client at exit: a body it could not read, a backend that was closing, or a hand-off it could not write"},
	{Key: "uploads_refused_buildid", Label: "uploads the client refused for a build id that disagrees with the key",
		Note: "the compiled object names another action than the key it was offered under; a mis-keyed object would poison every consumer, so the client keeps it local"},
	{Key: "uploads_refused_modindex", Label: "uploads the client refused as module indexes",
		Note: "a module index never belongs in a shared cache; the client recomputes it locally. A client older than the split reports both refusals here"},
	{Key: "evictions", Label: "objects evicted",
		Note: "each one is a miss for the next build that wanted it; the evicted row above counts the ones a build has asked for since"},
	{Key: "shed", Label: "requests shed at capacity",
		Note: "a shed upload is an object the cache never stored, and a shed read quiets the client for the Retry-After window"},
	{Key: "put_refused", Label: "uploads refused as module indexes",
		Note: "refused on purpose; the client recomputes a module index locally"},
	{Key: "put_digest_mismatch", Label: "uploads rejected for a bad digest",
		Note: "the bytes disagreed with the digest the uploader sent, so the wire corrupted them"},
	{Key: "put_errors", Label: "uploads that failed to store",
		Note: "a store error is an object the next build misses; the server log has the cause"},
	{Key: "xattrs_dropped", Label: "metadata dropped for lack of xattr space",
		Note: "the object stored without optional metadata; a build that needs it cannot use the hit"},
}

// dashboardMisses is the WHY MISSES panel.
type dashboardMisses struct {
	// Total is every miss the server knows of, the client's skips included.
	Total float64 `json:"total"`
	// Reasons holds every cause in display order, zero counts included, so a row keeps its place as counts arrive.
	Reasons []missReasonRow `json:"reasons"`
	// Dominant is the key of the reason with the most misses, or "" with none.
	Dominant string `json:"dominant"`
	// Verdict is the sentence the panel leads with.
	Verdict string `json:"verdict"`
	// Signals are the supply-side numbers that explain the causes.
	Signals []missSignal `json:"signals"`
	// Sample is what the server knows of the keys clients sampled from their index skips.
	Sample dashboardSkipSample `json:"sample"`
}

func labelCount(stats map[string]metricValue, name, label, value string) float64 {
	var n float64
	for _, p := range stats[name].Series {
		if p.Labels[label] == value {
			n += p.Value
		}
	}
	return n
}

func scalar(stats map[string]metricValue, name string) float64 {
	m := stats[name]
	if m.Value != nil {
		return *m.Value
	}
	var n float64
	for _, p := range m.Series {
		n += p.Value
	}
	return n
}

func skipCount(reason string) func(map[string]metricValue) float64 {
	return func(stats map[string]metricValue) float64 {
		var n float64
		for _, p := range stats["s3_client_skips_total"].Series {
			if p.Labels["op"] == "get" && p.Labels["reason"] == reason {
				n += p.Value
			}
		}
		return n
	}
}

// serverCount adds the batch reason and the single-GET outcome of one cause.
func serverCount(reason string) func(map[string]metricValue) float64 {
	return func(stats map[string]metricValue) float64 {
		return labelCount(stats, "s3_batch_misses_total", "reason", reason) +
			labelCount(stats, "s3_get_requests_total", "outcome", "miss_"+reason)
	}
}

// missReasonSpecs is every cause the panel shows, in display order. The ones
// that mean the cache works first, then capacity, then the ones that mean a bug.
var missReasonSpecs = []missReasonSpec{
	{
		key: "not_in_index", label: "never stored", source: missSourceClient, severity: missSeverityExpected,
		explain: "The client's copy of /_index did not list the key, so it built the object without asking. Nothing uploaded that key, or the sweeper removed it before this client fetched its index.",
		action:  "Expected on new code and new dependencies. If it dominates on repeat builds of unchanged code, uploads are not landing: check the upload signals below.",
		count:   skipCount("not_in_index"),
	},
	{
		key: "empty_index", label: "no index yet", source: missSourceClient, severity: missSeverityExpected,
		explain: "The client ran with an empty index. This is the first build against a fresh cache, or the client could not download /_index at all.",
		action:  "Expected once per fresh cache. On a cache that holds keys, check the Index route's status codes and whether the index download outlasts the client's silence window.",
		count:   skipCount("empty_index"),
	},
	{
		key: "batch_backoff", label: "client gave up probing", source: missSourceClient, severity: missSeverityExpected,
		explain: "The client saw several empty batch answers in a row and stopped asking for the rest of the run.",
		action:  "A build on new code trips this normally. If it rises on repeat builds, the index copy the client holds is stale or the uploads from the previous build never landed.",
		count:   skipCount("batch_backoff"),
	},
	{
		key: "known_miss", label: "asked before this run", source: missSourceClient, severity: missSeverityExpected,
		explain: "The server already said the key is absent in this run, so the client did not ask again.",
		action:  "A repeat of an earlier miss. It carries no new information; read the reason the first miss had.",
		count:   skipCount("known_miss"),
	},
	{
		key: missNotFound, label: "unknown key", source: missSourceServer, severity: missSeverityExpected,
		explain: "The client asked for a key this server never advertised. A client with no fresh index copy asks the server instead of its copy, and a request from an older client or curl asks for whatever it likes.",
		action:  "Expected while a client has no index. If it dominates with a fresh index on every client, the client and the server disagree on the key set: compare the index the client holds with /_index.",
		count:   serverCount(missNotFound),
	},
	{
		key: missEvicted, label: "evicted since the client's index", source: missSourceServer, severity: missSeverityCapacity,
		explain: "The key was in /_index when the client fetched it, and the sweeper removed it before the build asked. The cache held the object and let it go.",
		action:  "The cache is too small for the working set, or max_age is too short. Raise eviction.max_bytes, or stop evicting by age. The cache size tile says how full it is.",
		count:   serverCount(missEvicted),
	},
	{
		key: missAdvertised, label: "advertised but unservable", source: missSourceServer, severity: missSeverityBug,
		explain: "/_index advertises the key and the store has no body for it. Every client skips re-uploading an advertised key, so this is a permanent miss until the index is rebuilt.",
		action:  "A bug, not load. The index and the store diverged: an unlink outside the server, or a data_dir restored without its index rebuild. Restart the server to rebuild the index from disk.",
		count:   serverCount(missAdvertised),
	},
	{
		key: missModuleIndex, label: "module index refused on read", source: missSourceIntegrity, severity: missSeverityBug,
		explain: "The stored body is a Go module index, which must never be served from a shared cache. The read guard evicted it.",
		action:  "The PUT guard should have refused it. A count here means a body got past the PUT guard, or was written before the guard existed.",
		count:   serverCount(missModuleIndex),
	},
	{
		key: missPeekError, label: "read guard could not inspect the body", source: missSourceIntegrity, severity: missSeverityBug,
		explain: "The module-index guard could not read or rewind the body, so it refused to serve it.",
		action:  "Check the data_dir for a disk error. The server log names the key.",
		count:   serverCount(missPeekError),
	},
	{
		key: missSelfHeal, label: "body cannot be repaired", source: missSourceIntegrity, severity: missSeverityBug,
		explain: "The object carries no outputid and its body does not decompress, so no outputid can be reconstructed. The key was de-advertised.",
		action:  "The next build re-uploads a good body. A rising count means a client stores bodies this server cannot read.",
		count:   serverCount(missSelfHeal),
	},
	{
		key: missStoredDigest, label: "body changed on disk", source: missSourceIntegrity, severity: missSeverityBug,
		explain: "The stored body no longer hashes to the digest recorded with it, or vanished between the stat and the open. The server evicted it rather than serve corrupt bytes.",
		action:  "Disk rot or an unlink outside the server. Check the data_dir's filesystem health.",
		count:   serverCount(missStoredDigest),
	},
	{
		key: missStatError, label: "stat failed", source: missSourceIntegrity, severity: missSeverityBug,
		explain: "The store could not stat the key for a reason other than not found.",
		action:  "A permission or disk error in the data_dir. The server log names the key.",
		count:   serverCount(missStatError),
	},
}

// diagnoseMisses builds the panel from the flattened registry and the
// configuration the signals are read against.
func diagnoseMisses(stats map[string]metricValue, eviction dashboardEvictionInfo) dashboardMisses {
	out := dashboardMisses{Reasons: make([]missReasonRow, 0, len(missReasonSpecs))}
	for _, spec := range missReasonSpecs {
		n := spec.count(stats)
		out.Total += n
		out.Reasons = append(out.Reasons, missReasonRow{Key: spec.key, Count: n, Source: spec.source, Severity: spec.severity})
	}
	if out.Total > 0 {
		for i := range out.Reasons {
			out.Reasons[i].Share = out.Reasons[i].Count / out.Total
		}
		top := 0
		for i, r := range out.Reasons {
			if r.Count > out.Reasons[top].Count {
				top = i
			}
		}
		out.Dominant = out.Reasons[top].Key
	}
	out.Signals = missSignals(stats, eviction)
	out.Sample = skipSample(stats)
	// The sample's own explanation sits in out.Sample; the page shows it
	// under the never-stored cause.
	for i := range out.Reasons {
		if out.Reasons[i].Key == "not_in_index" && out.Sample.EvictedShare >= 0.5 {
			out.Reasons[i].Severity = missSeverityCapacity
		}
	}
	out.Verdict = missVerdict(out) + lostUploadsVerdict(out.Signals) + out.Sample.Verdict
	return out
}

// dashboardSkipSample is what the server knows of the keys clients sampled
// from their index skips: the share the sweeper had evicted. The share the
// server holds now, and the share it never held.
type dashboardSkipSample struct {
	Keys            float64 `json:"keys"`
	Evicted         float64 `json:"evicted"`
	Advertised      float64 `json:"advertised"`
	Unknown         float64 `json:"unknown"`
	EvictedShare    float64 `json:"evicted_share"`
	AdvertisedShare float64 `json:"advertised_share"`
	Explain         string  `json:"explain"`
	Verdict         string  `json:"verdict"`
}

// skipSample reads the sample verdicts. A never-stored miss the client
// answered from its own index copy is the one the server never sees. The
// sample is how it still gets to say what those keys were.
func skipSample(stats map[string]metricValue) dashboardSkipSample {
	s := dashboardSkipSample{
		Evicted:    labelCount(stats, "s3_client_skip_samples_total", "verdict", "evicted"),
		Advertised: labelCount(stats, "s3_client_skip_samples_total", "verdict", "advertised"),
		Unknown:    labelCount(stats, "s3_client_skip_samples_total", "verdict", "unknown"),
	}
	s.Keys = s.Evicted + s.Advertised + s.Unknown
	if s.Keys == 0 {
		return s
	}
	s.EvictedShare = s.Evicted / s.Keys
	s.AdvertisedShare = s.Advertised / s.Keys
	s.Explain = "Of " + formatCount(s.Keys) + " skipped keys clients sampled, " + formatShare(s.EvictedShare) + " were keys the sweeper had evicted, " +
		formatShare(s.AdvertisedShare) + " were keys this server holds now (the client's index copy was older than the upload), and the rest it never held."
	switch {
	case s.EvictedShare >= 0.5:
		s.Verdict = " The sample says most of the never-stored misses are evictions: the cache is too small for what the fleet builds, so raise eviction.max_bytes."
	case s.AdvertisedShare >= 0.5:
		s.Verdict = " The sample says most of the never-stored misses are keys the server holds now: clients run on an index copy older than the uploads, so the index max age is too long for how fast the fleet uploads."
	}
	return s
}

// lostUploadsVerdict is the sentence added when a client reported uploads
// it never completed. Those keys miss on every later build, so they explain
// a never-stored count that does not fall on repeat builds.
func lostUploadsVerdict(signals []missSignal) string {
	var failed, abandoned float64
	for _, s := range signals {
		switch s.Key {
		case "uploads_failed":
			failed = s.Count
		case "uploads_abandoned":
			abandoned = s.Count
		}
	}
	if failed+abandoned == 0 {
		return ""
	}
	return " Clients reported " + formatCount(failed+abandoned) + " uploads that never reached the store (" +
		formatCount(failed) + " failed, " + formatCount(abandoned) + " abandoned); each of those keys misses on every later build until something uploads it again."
}

// missVerdict is the sentence the panel leads with: the dominant cause, its
// share, and what to do about it. A bug outranks a bigger expected count,
// because an expected miss needs nobody and a bug needs somebody.
func missVerdict(m dashboardMisses) string {
	if m.Total == 0 {
		return "No miss recorded yet. A build that asks for a key the cache lacks shows here, as do the misses a client answers from its own index copy."
	}
	rows := append([]missReasonRow(nil), m.Reasons...)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Count > rows[j].Count })
	var dominant missReasonRow
	for _, r := range rows {
		if r.Count > 0 && r.Key == m.Dominant {
			dominant = r
			break
		}
	}
	var bug *missReasonRow
	for i := range rows {
		if rows[i].Count > 0 && rows[i].Severity == missSeverityBug {
			bug = &rows[i]
			break
		}
	}
	text := missReasonSpecByKey(dominant.Key)
	lead := formatShare(dominant.Share) + " of misses: " + text.label + ". " + text.action
	if bug == nil || bug.Key == dominant.Key {
		return lead
	}
	bugText := missReasonSpecByKey(bug.Key)
	return lead + " Also " + formatCount(bug.Count) + " " + bugText.label + ", which is a bug: " + bugText.action
}

// missSignals are the supply-side numbers: an object never uploaded is a
// miss on every later build. Each of these is a way an upload is lost.
func missSignals(stats map[string]metricValue, eviction dashboardEvictionInfo) []missSignal {
	cacheBytes := scalar(stats, "s3_cache_bytes")
	fill := missSignal{Key: "cache_fill", Note: "no size budget: nothing is evicted for size"}
	if eviction.Enabled && eviction.MaxBytes > 0 {
		fill.Count = cacheBytes / float64(eviction.MaxBytes)
		fill.Alert = fill.Count > 0.9
		fill.Note = byteSize(int64(cacheBytes)) + " of " + byteSize(eviction.MaxBytes) + ". A full cache evicts on every sweep, and every eviction is a later miss."
		if cacheBytes == 0 {
			fill.Note = "not measured until the first eviction sweep"
		}
	}
	evictions := scalar(stats, "s3_evictions_total")
	refusals := scalar(stats, "s3_put_refusals_total")
	putMismatch := labelCount(stats, "s3_stored_digest_mismatch_total", "stage", "put")
	shed := scalar(stats, "s3_http_rejected_total")
	dropped := scalar(stats, "s3_metadata_xattrs_dropped_total")
	putErrors := labelCount(stats, "cache_storage_operations_total", "operation", "put") -
		labelCountBoth(stats, "cache_storage_operations_total", "operation", "put", "status", "ok")
	lostFailed := labelCount(stats, "s3_client_uploads_lost_total", "reason", "failed")
	lostAbandoned := labelCount(stats, "s3_client_uploads_lost_total", "reason", "abandoned")
	lostBuildID := labelCount(stats, "s3_client_uploads_lost_total", "reason", "refused_buildid")
	lostModIndex := labelCount(stats, "s3_client_uploads_lost_total", "reason", "refused_modindex") +
		labelCount(stats, "s3_client_uploads_lost_total", "reason", "refused")
	// The order is missSignalVocab's order.
	return []missSignal{
		fill,
		{Key: "uploads_failed", Count: lostFailed, Alert: lostFailed > 0},
		{Key: "uploads_abandoned", Count: lostAbandoned, Alert: lostAbandoned > 0},
		{Key: "uploads_refused_buildid", Count: lostBuildID, Alert: lostBuildID > 0},
		{Key: "uploads_refused_modindex", Count: lostModIndex},
		{Key: "evictions", Count: evictions},
		{Key: "shed", Count: shed, Alert: shed > 0},
		{Key: "put_refused", Count: refusals},
		{Key: "put_digest_mismatch", Count: putMismatch, Alert: putMismatch > 0},
		{Key: "put_errors", Count: putErrors, Alert: putErrors > 0},
		{Key: "xattrs_dropped", Count: dropped, Alert: dropped > 0},
	}
}

func labelCountBoth(stats map[string]metricValue, name, l1, v1, l2, v2 string) float64 {
	var n float64
	for _, p := range stats[name].Series {
		if p.Labels[l1] == v1 && p.Labels[l2] == v2 {
			n += p.Value
		}
	}
	return n
}

func formatShare(share float64) string {
	return strconv.FormatFloat(share*100, 'f', 1, 64) + "%"
}

// formatCount renders a count with no exponent and no spurious decimals.
func formatCount(n float64) string {
	return strconv.FormatFloat(n, 'f', -1, 64)
}
