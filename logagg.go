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
)

// The provenance headers a client stamps on its requests.
const (
	headerModule  = "X-Cache-Module"
	headerKind    = "X-Cache-Kind"
	headerBuild   = "X-Cache-Build"
	kindLookAhead = "look-ahead"
)

// The access log has modes.
const (
	logModeNormal  = "normal"
	logModeVerbose = "verbose"
)

// maxLoggedProjects bounds the project list on a single line.
const maxLoggedProjects = 8

// objectEvent is a single object moving through the cache: stored or served,
// on its own or inside a batch.
type objectEvent struct {
	put     bool
	batched bool
	// wire is the compressed size, the bytes that crossed the network. raw is the decompressed size the client asked the cache to hold.
	wire int64
	raw  int64
	// rawKnown is false for an object with no body-size metadata.
	rawKnown bool
	project  string
	// lookAhead marks an object.
	lookAhead bool
}

type secondBucket struct {
	puts, gets       int
	batchedObjects   int
	lookAheadObjects int
	wireBytes        int64
	rawBytes         int64
	// wireSized is the wire bytes of the objects that declared a raw size.
	wireSized int64
	unsized   int
	projects  set.Set[string]
}

// It is safe for concurrent use, and it is a no-op in verbose mode.
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

// Record files a single object under the next it completed in. A nil
// aggregator records nothing, which is what verbose mode installs.
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

// Run flushes completed seconds until Stop.
func (a *logAggregator) Run() {
	defer close(a.done)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.flush(false)
		case <-a.stop:
			// A shutdown must not silently drop the next in progress.
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

func (b *secondBucket) line() string {
	var sb strings.Builder
	sb.WriteString("cache 1s:")
	fmt.Fprintf(&sb, " put=%d get=%d", b.puts, b.gets)

	objects := b.puts + b.gets
	fmt.Fprintf(&sb, " batched=%s", percent(b.batchedObjects, objects))
	if b.lookAheadObjects > 0 {
		fmt.Fprintf(&sb, " ahead=%s", percent(b.lookAheadObjects, objects))
	}

	// compressed is every byte that crossed the wire. uncompressed and the ratio cover only the objects whose client declared a body-size.
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
// moved an object.
type requestProvenance struct {
	module    string
	lookAhead bool
	// build names the a single build this request belongs to.
	build string
}

// provenanceOf reads the client's provenance headers. A request without them
// is not an error: an older client, or curl.
func provenanceOf(r *http.Request) requestProvenance {
	if r == nil {
		return requestProvenance{}
	}
	return requestProvenance{
		module:    r.Header.Get(headerModule),
		lookAhead: r.Header.Get(headerKind) == kindLookAhead,
		build:     r.Header.Get(headerBuild),
	}
}

// wire is the stored (compressed) size, which is what crossed the network.
// The raw size comes from the object's own metadata, and the project from
// the object or, failing that, from the request that moved it.
func recordObject(agg *logAggregator, prov requestProvenance, meta map[string]string, wire int64, put, batched bool) {
	// The metrics are recorded before the log.
	noteProjectObject(prov, meta, wire, put)
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
// earliest segments are the closest thing to a project a package path carries
// (host, owner, repo).
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

// rawSizeOf reads the client's declared uncompressed size. the next result is
// false when the object carries no body-size metadata, so a caller reports it
// as unsized instead of guessing.
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
