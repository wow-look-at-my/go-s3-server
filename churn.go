// Why a package is stored again: what differed between its last uploads.

package main

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A build that stores a package the cache already held under another key compiled it again.

// churnKind is the label the tracker reports its size under.
const churnKind = "package-churn"

// churnEntryBytes is the fixed cost of one tracked package, beside its strings.
const churnEntryBytes = 160

// churnRecentMax bounds the examples the panel shows.
const churnRecentMax = 40

// churnTopMax bounds the packages the panel ranks.
const churnTopMax = 20

// The change kinds, in the order the panel shows them. The first field that
// differs names the kind, so one re-store has one kind.
const (
	changeGoVersion = "go_version"
	changeToolchain = "toolchain"
	changeTarget    = "target"
	changeSources   = "sources"
	changeInputs    = "inputs" // every field agrees and the output is the same: an input the metadata does not carry
	changeOutput    = "output" // every field agrees and the output differs: a file's content, or a dependency
)

var changeKinds = []string{changeGoVersion, changeToolchain, changeTarget, changeSources, changeInputs, changeOutput}

// packageRestoresTotal counts packages stored again under a new key, by what
// differed and whether the compiled output did.
var packageRestoresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_package_restores_total",
	Help: "Packages stored again under a new action id, by the field that differed from the last upload (go_version, toolchain, target, sources, inputs, output) and by output (same, changed).",
}, []string{"change", "output"})

// packageSeen is the last upload or hit of one package.
type packageSeen struct {
	hash      [gbciHashSize]byte
	content   string
	goVersion string
	toolchain string
	target    string
	src       string
	project   string
	at        int64
	// restores and sameOutput count the re-stores seen for this package since the server started.
	restores   uint32
	sameOutput uint32
	lastChange string
}

func (p *packageSeen) bytes() int64 {
	return churnEntryBytes + int64(len(p.content)+len(p.goVersion)+len(p.toolchain)+len(p.target)+len(p.src)+len(p.project)+len(p.lastChange))
}

// fieldDiff is one metadata field that differed between uploads.
type fieldDiff struct {
	Field string `json:"field"`
	From  string `json:"from"`
	To    string `json:"to"`
}

// restoreEvent is one re-store, as the panel lists it.
type restoreEvent struct {
	At         time.Time   `json:"at"`
	Project    string      `json:"project"`
	Package    string      `json:"pkg"`
	Target     string      `json:"target,omitempty"`
	Change     string      `json:"change"`
	Output     string      `json:"output"`
	SameOutput bool        `json:"same_output"`
	Diffs      []fieldDiff `json:"diffs"`
}

// churnTracker remembers the last upload of every package it has seen and
// classifies each re-store.
type churnTracker struct {
	entries *lruCache[string, *packageSeen]
	mu      sync.Mutex
	// recent is a ring of the last re-stores, newest last.
	recent   []restoreEvent
	total    uint64
	same     uint64
	unknown  uint64
	byChange map[string]uint64
	now      func() time.Time
}

func newChurnTracker(budget int64) *churnTracker {
	return &churnTracker{
		entries: newLRUCache(budget,
			func(k string) uint32 { return fnv1a(k) },
			func(k string, p *packageSeen) int64 { return int64(len(k)) + p.bytes() }),
		byChange: map[string]uint64{},
		now:      time.Now,
	}
}

// packageChurn is the server's one tracker. NewServer sizes it.
var packageChurn = newChurnTracker(defaultChurnBytes)

// observe records an object that moved. A put of a package the tracker
// knows under another key is a re-store. A hit refreshes what the tracker
// knows, so the next put compares against the newest object the cache
// served, whoever stored it.
func (c *churnTracker) observe(key string, meta map[string]string, put bool, project string) {
	pkg := meta["pkg"]
	if pkg == "" || pkg == "main" || pkg == "command-line-arguments" {
		// A main package of a test or of a directory build carries the same import path as every other one.
		return
	}
	hash, ok := extractActionHash(key)
	if !ok {
		return
	}
	cur := &packageSeen{
		hash:      hash,
		content:   meta["content-id"],
		goVersion: meta["go-version"],
		toolchain: meta["toolchain-version"],
		target:    meta["target"],
		src:       meta["src"],
		project:   project,
		at:        c.now().Unix(),
	}
	k := pkg + "\x00" + cur.target
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, known := c.entries.Get(k)
	if !known {
		c.entries.Put(k, cur)
		return
	}
	cur.restores, cur.sameOutput, cur.lastChange = prev.restores, prev.sameOutput, prev.lastChange
	if prev.hash == hash || !put {
		c.entries.Put(k, cur)
		return
	}
	diffs, change := classifyChange(prev, cur)
	output := outputVerdict(prev, cur)
	same := output == outputSame
	cur.restores++
	cur.lastChange = change
	if same {
		cur.sameOutput++
	}
	c.entries.Put(k, cur)
	c.total++
	c.byChange[change]++
	if same {
		c.same++
	}
	if output == outputUnknown {
		c.unknown++
	}
	packageRestoresTotal.WithLabelValues(change, output).Inc()
	projectTotals.noteRestore(project, same)
	ev := restoreEvent{At: time.Unix(cur.at, 0).UTC(), Project: project, Package: pkg, Target: cur.target, Change: change, Output: output, SameOutput: same, Diffs: diffs}
	if len(c.recent) >= churnRecentMax {
		copy(c.recent, c.recent[1:])
		c.recent = c.recent[:churnRecentMax-1]
	}
	c.recent = append(c.recent, ev)
}

// The output label of a re-store. cmd/go stamps the action id into every
// archive, so the stored bytes always differ between keys.
const (
	outputSame    = "same"
	outputChanged = "changed"
	outputUnknown = "unknown" // one of the uploads carried no content hash
)

func outputVerdict(prev, cur *packageSeen) string {
	switch {
	case prev.content == "" || cur.content == "":
		return outputUnknown
	case prev.content == cur.content:
		return outputSame
	default:
		return outputChanged
	}
}

// classifyChange lists the fields that differ and names the kind by the
// first of them.
func classifyChange(prev, cur *packageSeen) ([]fieldDiff, string) {
	diffs := []fieldDiff{}
	add := func(field, from, to string) {
		if from != to {
			diffs = append(diffs, fieldDiff{Field: field, From: from, To: to})
		}
	}
	add("go-version", prev.goVersion, cur.goVersion)
	add("toolchain-version", prev.toolchain, cur.toolchain)
	add("target", prev.target, cur.target)
	add("src", prev.src, cur.src)
	switch {
	case len(diffs) > 0:
		return diffs, map[string]string{"go-version": changeGoVersion, "toolchain-version": changeToolchain, "target": changeTarget, "src": changeSources}[diffs[0].Field]
	case outputVerdict(prev, cur) == outputSame:
		return diffs, changeInputs
	default:
		return diffs, changeOutput
	}
}

// churnCause is one change kind on the panel.
type churnCause struct {
	Key     string  `json:"key"`
	Label   string  `json:"label"`
	Count   float64 `json:"count"`
	Share   float64 `json:"share"`
	Explain string  `json:"explain"`
	Action  string  `json:"action"`
}

// churnPackage is one ranked package.
type churnPackage struct {
	Package    string `json:"pkg"`
	Target     string `json:"target,omitempty"`
	Project    string `json:"project"`
	Restores   uint32 `json:"restores"`
	SameOutput uint32 `json:"same_output"`
	LastChange string `json:"last_change"`
}

// dashboardChurn is the WHAT CHANGED panel.
type dashboardChurn struct {
	// Tracked is how many packages the tracker holds a last upload for.
	Tracked int `json:"tracked_packages"`
	// Restores is every package stored again under a new key since the server started.
	Restores uint64 `json:"restores"`
	// SameOutput is how many of those compiled to the identical content.
	SameOutput uint64 `json:"same_output"`
	// UnknownOutput is how many came from a client that sent no content hash, so the question has no answer.
	UnknownOutput uint64         `json:"unknown_output"`
	Causes        []churnCause   `json:"causes"`
	Top        []churnPackage `json:"top"`
	Recent     []restoreEvent `json:"recent"`
	Verdict    string         `json:"verdict"`
}

var churnCauseSpecs = map[string]churnCause{
	changeGoVersion: {Label: "Go version changed",
		Explain: "The package was last stored by another Go version. The compiler's own build id is part of every action id, so a new Go recompiles everything once.",
		Action:  "Expected once per Go upgrade. If versions alternate, two fleets build with different toolchains and neither shares with the other."},
	changeToolchain: {Label: "client version changed",
		Explain: "The go-toolchain client that uploaded it changed. A client release that carries a new compiler build changes every action id.",
		Action:  "Expected once per client release. If it alternates, pin the fleet to one release."},
	changeTarget: {Label: "target changed",
		Explain: "The same package was stored for another GOOS/GOARCH.",
		Action:  "Expected when a fleet builds for several targets."},
	changeSources: {Label: "source files changed",
		Explain: "A Go file was added to or removed from the package.",
		Action:  "Expected on edited code. The list is the first few names, so a change past them reads as unchanged files."},
	changeInputs: {Label: "same sources, same content, new key",
		Explain: "The Go version, the client, the target and the files all agree with the last upload, and the compiled content is the same. An input the metadata does not carry changed: a build flag, an environment variable, GOFLAGS, a -ldflags stamp, a path without -trimpath, or a dependency's build id.",
		Action:  "This is the pointless re-store. Compare the two builds' go env, GOFLAGS and the flags in the build command; what differs between them is in the action id and not in the output."},
	changeOutput: {Label: "same files, content changed or unknown",
		Explain: "The Go version, the client, the target and the file names agree with the last upload, but the compiled content differs, or one upload carried no content hash. A file's content changed, or a dependency's output changed and this package followed it.",
		Action:  "Expected on edited code. On unchanged code, look for a generated file or a dependency that is stored again on every build. A re-store with an unknown output came from a client older than the content hash; an upgraded client answers it."},
}

// snapshot builds the panel.
func (c *churnTracker) snapshot() dashboardChurn {
	c.mu.Lock()
	out := dashboardChurn{
		Tracked:       c.entries.Len(),
		Restores:      c.total,
		SameOutput:    c.same,
		UnknownOutput: c.unknown,
		Recent:        make([]restoreEvent, 0, len(c.recent)),
	}
	for i := len(c.recent) - 1; i >= 0; i-- {
		out.Recent = append(out.Recent, c.recent[i])
	}
	for _, kind := range changeKinds {
		cause := churnCauseSpecs[kind]
		cause.Key = kind
		cause.Count = float64(c.byChange[kind])
		if c.total > 0 {
			cause.Share = cause.Count / float64(c.total)
		}
		out.Causes = append(out.Causes, cause)
	}
	c.mu.Unlock()

	var ranked []churnPackage
	c.entries.Range(func(k string, p *packageSeen) {
		if p.restores == 0 {
			return
		}
		pkg, target, _ := strings.Cut(k, "\x00")
		ranked = append(ranked, churnPackage{Package: pkg, Target: target, Project: p.project, Restores: p.restores, SameOutput: p.sameOutput, LastChange: p.lastChange})
	})
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Restores != ranked[j].Restores {
			return ranked[i].Restores > ranked[j].Restores
		}
		return ranked[i].Package < ranked[j].Package
	})
	if len(ranked) > churnTopMax {
		ranked = ranked[:churnTopMax]
	}
	out.Top = ranked
	out.Verdict = churnVerdict(out)
	return out
}

// churnVerdict is the sentence the panel leads with.
func churnVerdict(c dashboardChurn) string {
	if c.Restores == 0 {
		return "No package has been stored again under a new key since the server started. The tracker learns a package from its first upload or hit, so this fills as builds run."
	}
	top := c.Causes[0]
	for _, cause := range c.Causes {
		if cause.Count > top.Count {
			top = cause
		}
	}
	lead := formatCount(float64(c.Restores)) + " packages were stored again under a new key, " +
		formatCount(float64(c.SameOutput)) + " of them (" + formatShare(float64(c.SameOutput)/float64(c.Restores)) + ") compiled to the identical content"
	if c.UnknownOutput > 0 {
		lead += ", and " + formatCount(float64(c.UnknownOutput)) + " came from a client that sent no content hash, so for those the question has no answer"
	}
	lead += ". " + formatShare(top.Share) + " of the re-stores: " + top.Label + ". " + top.Action
	if ex := latestExample(c.Recent, top.Key); ex != "" {
		lead += " Latest: " + ex
	}
	return lead
}

// latestExample names the newest re-store of a kind with the values that differed.
func latestExample(recent []restoreEvent, kind string) string {
	for _, ev := range recent {
		if ev.Change != kind {
			continue
		}
		s := ev.Package
		for _, d := range ev.Diffs {
			s += " " + d.Field + " " + d.From + " -> " + d.To
		}
		if len(ev.Diffs) == 0 {
			s += " (content " + ev.Output + ")"
		}
		return s + "."
	}
	return ""
}
