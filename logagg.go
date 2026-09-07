package main

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// The access log has two modes.
//
// verbose prints one line per HTTP request, and only one. A handler that has
// something to add (a batch's key counts, a refusal) attaches it to that same
// line, so a single request never appears twice under two spellings.
//
// normal prints nothing per request. It prints one line per SECOND in which
// the cache moved objects: how many were stored and served, how much of that
// went through the batch endpoints, the byte rates on and off the wire, and
// which projects the traffic belonged to. A CI fleet issues thousands of
// requests a second, so a per-request log is unreadable exactly when it is
// most needed.
const (
	logModeNormal  = "normal"
	logModeVerbose = "verbose"
)

// maxLoggedProjects bounds the project list on one line. Past it the line says
// how many more there were, rather than running to the width of the terminal.
const maxLoggedProjects = 8

// objectEvent is one object moving through the cache: stored or served, on its
// own or inside a batch.
type objectEvent struct {
	put     bool
	batched bool
	// wire is the compressed size, the bytes that crossed the network. raw is
	// the decompressed size the client asked the cache to hold, taken from the
	// body-size metadata the client sends.
	wire int64
	raw  int64
	// rawKnown is false for an object with no body-size metadata. Such an
	// object counts toward the rates that do not need it, and is reported as
	// unsized instead of being folded into the compression ratio as if it had
	// compressed to nothing.
	rawKnown bool
	project  string
	// lookAhead marks an object the client fetched before anything asked for
	// it. A second in which most of the traffic is look-ahead is a cache
	// working ahead of a build, not a build waiting on a cache, and a log that
	// cannot tell those apart reports the two identically.
	lookAhead bool
}

// secondBucket accumulates one second of objectEvents.
type secondBucket struct {
	puts, gets       int
	batchedObjects   int
	lookAheadObjects int
	wireBytes      int64
	rawBytes       int64
	// wireSized is the wire bytes of the objects that declared a raw size, so
	// the compression ratio divides like against like.
	wireSized int64
	unsized   int
	projects  set.Set[string]
}

// logAggregator turns objectEvents into one line per active second. It is safe
// for concurrent use, and it is a no-op in verbose mode.
type logAggregator struct {
	mu      sync.Mutex
	buckets map[int64]*secondBucket
	printf  func(format string, v ...any)
	now     func() time.Time
	stop    chan struct{}
	done    chan struct{}
}

func newLogAggregator() *logAggregator {
	return &logAggregator{
		buckets: make(map[int64]*secondBucket),
		printf:  log.Printf,
		now:     time.Now,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Record files one object under the second it completed in. A nil aggregator
// records nothing, which is what verbose mode installs.
func (a *logAggregator) Record(ev objectEvent) {
	if a == nil {
		return
	}
	now := a.now()
	sec := now.Unix()

	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.buckets[sec]
	if b == nil {
		b = &secondBucket{projects: set.New[string]()}
		a.buckets[sec] = b
	}
	if ev.put {
		b.puts++
	} else {
		b.gets++
	}
	if ev.batched {
		b.batchedObjects++
	}
	if ev.lookAhead {
		b.lookAheadObjects++
	}
	b.wireBytes += ev.wire
	if ev.rawKnown {
		b.rawBytes += ev.raw
		b.wireSized += ev.wire
	} else {
		b.unsized++
	}
	if ev.project != "" {
		b.projects.Add(ev.project)
	}
}

// Run flushes completed seconds until Stop. It ticks faster than one second so
// a bucket is emitted promptly after its second ends, rather than waiting out
// a full period that started at an arbitrary offset.
func (a *logAggregator) Run() {
	defer close(a.done)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.flush(false)
		case <-a.stop:
			// A shutdown must not silently drop the second in progress.
			a.flush(true)
			return
		}
	}
}

func (a *logAggregator) Stop() {
	if a == nil {
		return
	}
	close(a.stop)
	<-a.done
}

// flush emits every bucket whose second has passed. With all set, it emits the
// current second too.
func (a *logAggregator) flush(all bool) {
	cutoff := a.now().Unix()

	a.mu.Lock()
	ready := make([]int64, 0, len(a.buckets))
	for sec := range a.buckets {
		if all || sec < cutoff {
			ready = append(ready, sec)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	lines := make([]string, 0, len(ready))
	for _, sec := range ready {
		lines = append(lines, a.buckets[sec].line())
		delete(a.buckets, sec)
	}
	a.mu.Unlock()

	for _, line := range lines {
		a.printf("%s", line)
	}
}

// line renders one second. Every rate is per second by construction, because
// the bucket IS one second.
func (b *secondBucket) line() string {
	var sb strings.Builder
	sb.WriteString("cache 1s:")
	fmt.Fprintf(&sb, " put=%d get=%d", b.puts, b.gets)

	objects := b.puts + b.gets
	fmt.Fprintf(&sb, " batched=%s", percent(b.batchedObjects, objects))
	if b.lookAheadObjects > 0 {
		fmt.Fprintf(&sb, " ahead=%s", percent(b.lookAheadObjects, objects))
	}

	// compressed is every byte that crossed the wire. uncompressed and the
	// ratio cover only the objects whose client declared a body-size, because
	// nothing else can be compared. When those sets differ, sized=N/total says
	// so: without it the line reads as uncompressed being SMALLER than
	// compressed, which no compressor does.
	fmt.Fprintf(&sb, " compressed=%s/s", byteSize(b.wireBytes))
	sized := objects - b.unsized
	if sized > 0 {
		fmt.Fprintf(&sb, " uncompressed=%s/s ratio=%s", byteSize(b.rawBytes), percentInt64(b.wireSized, b.rawBytes))
	}
	if b.unsized > 0 {
		fmt.Fprintf(&sb, " sized=%d/%d", sized, objects)
	}

	if !b.projects.IsEmpty() {
		sb.WriteString(" projects=")
		sb.WriteString(joinProjects(b.projects))
	}
	return sb.String()
}

func percent(part, whole int) string {
	if whole == 0 {
		return "n/a"
	}
	return strconv.Itoa(int((float64(part)/float64(whole))*100+0.5)) + "%"
}

func percentInt64(part, whole int64) string {
	if whole == 0 {
		return "n/a"
	}
	return strconv.Itoa(int((float64(part)/float64(whole))*100+0.5)) + "%"
}

// joinProjects renders the project set: sorted for a stable line, and bounded
// so one busy second cannot print a screenful of module paths.
func joinProjects(projects set.Set[string]) string {
	names := projects.Values()
	sort.Strings(names)
	if len(names) <= maxLoggedProjects {
		return strings.Join(names, ", ")
	}
	extra := len(names) - maxLoggedProjects
	return strings.Join(names[:maxLoggedProjects], ", ") + fmt.Sprintf(", +%d more", extra)
}

// byteSize renders a byte count in binary units.
func byteSize(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + "B"
	}
	value := float64(n)
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	i := -1
	for value >= unit && i < len(units)-1 {
		value /= unit
		i++
	}
	if value < 10 {
		return strconv.FormatFloat(value, 'f', 1, 64) + units[i]
	}
	return strconv.FormatFloat(value, 'f', 0, 64) + units[i]
}

// requestProvenance is what a client said about itself on the request that
// moved an object. An object's own metadata says what it IS; these headers say
// who wanted it, which is the question a log about traffic answers.
type requestProvenance struct {
	module    string
	lookAhead bool
}

// provenanceOf reads the client's provenance headers. A request without them
// is not an error: an older client, or curl.
func provenanceOf(r *http.Request) requestProvenance {
	if r == nil {
		return requestProvenance{}
	}
	return requestProvenance{
		module:    r.Header.Get(cacheclient.HeaderModule),
		lookAhead: r.Header.Get(cacheclient.HeaderKind) == cacheclient.KindLookAhead,
	}
}

// recordObject files one object move under the current second. wire is the
// stored (compressed) size, which is what crossed the network. The raw size
// comes from the object's own metadata, and the project from the object or,
// failing that, from the request that moved it.
func recordObject(agg *logAggregator, prov requestProvenance, meta map[string]string, wire int64, put, batched bool) {
	if agg == nil {
		return
	}
	raw, known := rawSizeOf(meta)
	agg.Record(objectEvent{
		put:       put,
		batched:   batched,
		wire:      wire,
		raw:       raw,
		rawKnown:  known,
		project:   projectOf(meta, prov),
		lookAhead: prov.lookAhead,
	})
}

// projectOf names the project an object belongs to. The object's own module
// metadata is the answer whenever it is there. Otherwise the import path's
// first three segments are the closest thing to a project a package path
// carries (host, owner, repo). Failing both, the requesting client's own module
// header answers: a served object may carry no metadata at all, and a build
// asking for it is still a build belonging to some project.
func projectOf(meta map[string]string, prov requestProvenance) string {
	if module := meta["module"]; module != "" {
		return module
	}
	if pkg := meta["pkg"]; pkg != "" {
		parts := strings.Split(pkg, "/")
		if len(parts) > 3 {
			parts = parts[:3]
		}
		return strings.Join(parts, "/")
	}
	return prov.module
}

// rawSizeOf reads the client's declared uncompressed size. The second result
// is false when the object carries no body-size metadata, so a caller reports
// it as unsized instead of guessing.
func rawSizeOf(meta map[string]string) (int64, bool) {
	if meta == nil {
		return 0, false
	}
	raw := meta["body-size"]
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
