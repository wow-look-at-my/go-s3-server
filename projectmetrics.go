package main

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/wow-look-at-my/go-containers/set"
)

// Per-project cache traffic. Every client stamps X-Cache-Module on its
// requests, and projectOf turns that, plus the object's own module metadata,
// into the name of the project whose build moved the object.
//
// The question they answer is the thing a shared cache gets asked most: the
// hit rate is fine overall, so WHOSE builds are the ones missing?

// A label value the CLIENT chooses is a cardinality risk: a single typo per
// build and the registry grows a series that never goes away. The name set is
// therefore bounded, and a name past the bound is folded rather than dropped,
// so its traffic still shows up in the total.
const (
	// maxProjectLabels is how many distinct project names get their own
	// series. A fleet has tens of repositories, so this is generous.
	maxProjectLabels = 64
	// maxProjectNameLen bounds a single name. A module path is well under
	// it, and anything longer is not a module path.
	maxProjectNameLen = 128
	otherProject      = "other"
	// unknownProject takes the traffic of a request that named no project at
	// all: an older client, or curl.
	unknownProject = "unknown"
)

// Object kinds, which is what separates the traffic a build waited on from
// the traffic the cache moved on its own.
const (
	objKindHit       = "hit"       // a stored object served to a build that asked
	objKindLookAhead = "lookahead" // an object the server sent before anything asked
	objKindPut       = "put"       // an object a build stored
	objKindMiss      = "miss"      // a key asked for that the cache could not serve
)

var (
	// projectObjectsTotal is the object count. A project's miss rate is
	// miss / (hit + miss) over this a single metric.
	projectObjectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_project_objects_total",
		Help: "Cache objects moved, by project and kind (hit, lookahead, put, miss).",
	}, []string{"project", "kind"})

	// projectBytesTotal is the wire bytes those objects carried, which is what a
	// transfer budget is spent on. A miss carries none, so it contributes
	// nothing here.
	projectBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_project_bytes_total",
		Help: "Wire bytes moved, by project and kind (hit, lookahead, put).",
	}, []string{"project", "kind"})

	// projectNamesGauge is how many names hold a series of their own. It
	// climbing toward maxProjectLabels is the warning that the next new
	// project lands in "other".
	projectNamesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_project_names",
		Help: "Distinct project names holding their own metric series.",
	})

	// projectNamesFoldedTotal counts the events whose project name did not fit
	// the bound and went to "other". A nonzero value means the breakdown
	// below is incomplete. A silent fold would hide that.
	projectNamesFoldedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_project_names_folded_total",
		Help: "Events whose project name was folded into \"other\" because the name bound was reached.",
	})
)

// projectLabeller bounds the distinct project label values. A name it has
// already admitted keeps its series forever; a new name past the bound folds
// into "other".
type projectLabeller struct {
	mu   sync.Mutex
	seen set.Set[string]
}

func newProjectLabeller() *projectLabeller {
	return &projectLabeller{seen: set.New[string](maxProjectLabels)}
}

// label answers the metric label for a project name. An empty name is
// "unknown", a name that does not look like a single is "other", and a new
// name past the bound is "other" with the fold counted.
func (p *projectLabeller) label(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return unknownProject
	}
	if !plausibleProjectName(name) {
		projectNamesFoldedTotal.Inc()
		return otherProject
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen.Contains(name) {
		return name
	}
	if p.seen.Len() >= maxProjectLabels {
		projectNamesFoldedTotal.Inc()
		return otherProject
	}
	p.seen.Add(name)
	projectNamesGauge.Set(float64(p.seen.Len()))
	return name
}

// plausibleProjectName reports whether a name is short enough and printable
// enough to be a module path.
func plausibleProjectName(name string) bool {
	if len(name) > maxProjectNameLen {
		return false
	}
	for _, r := range name {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// projects is the server's a single labeller. Bounding is per process,
// which is where the registry lives.
var projects = newProjectLabeller()

// noteProjectObject counts a single object against the project that moved
// it. It sits beside recordObject, on the same events, so the log and the
// metrics can never disagree about who moved what.
func noteProjectObject(prov requestProvenance, meta map[string]string, wire int64, put bool) {
	kind := objKindHit
	switch {
	case put:
		kind = objKindPut
	case prov.lookAhead:
		kind = objKindLookAhead
	}
	label := projects.label(projectOf(meta, prov))
	projectObjectsTotal.WithLabelValues(label, kind).Inc()
	if wire > 0 {
		projectBytesTotal.WithLabelValues(label, kind).Add(float64(wire))
	}
}

// noteProjectMiss counts keys the cache could not serve against the project
// that asked for them. A miss carries no object and no metadata, so the
// requesting client's own module header is the only thing that names it.
func noteProjectMiss(prov requestProvenance, keys int) {
	if keys <= 0 {
		return
	}
	projectObjectsTotal.WithLabelValues(projects.label(prov.module), objKindMiss).Add(float64(keys))
}
