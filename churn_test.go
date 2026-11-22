package main

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testChurn() *churnTracker {
	c := newChurnTracker(1 << 20)
	at := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { at = at.Add(time.Second); return at }
	return c
}

// pkgMeta is an upload's metadata. content is the build id's content hash,
// which is what says whether uploads compiled the same thing.
func pkgMeta(content, goVersion, toolchain, src string) map[string]string {
	return map[string]string{
		"pkg": "example.com/m/p", "target": "linux/amd64", "outputid": "stamped-" + content,
		"content-id": content, "go-version": goVersion, "toolchain-version": toolchain, "src": src,
	}
}

// A key that changed with only the source directory names the move, with
// both directories, and not "content changed". The absolute path is in the
// object, so its content hash differs as well. An upload without the field
// is no evidence of a move.
func TestChurnNamesABuildFromAnotherDirectory(t *testing.T) {
	c := testChurn()
	at := func(dir string) map[string]string {
		meta := pkgMeta("", "go1.27", "r100", "a.go")
		meta["content-id"] = "content-" + dir
		if dir != "" {
			meta["src-dir"] = dir
		}
		return meta
	}
	c.observe(cacheprogKey('a'), at("/home/runner/work/m/m/p"), true, "example.com/m")
	c.observe(cacheprogKey('b'), at("/tmp/checkout-41/p"), true, "example.com/m")
	s := c.snapshot()
	require.EqualValues(t, 1, s.Restores)
	require.Len(t, s.Recent, 1)
	assert.Equal(t, changePath, s.Recent[0].Change)
	assert.Equal(t, []fieldDiff{{Field: "src-dir", From: "/home/runner/work/m/m/p", To: "/tmp/checkout-41/p"}}, s.Recent[0].Diffs)

	c.observe(cacheprogKey('c'), at(""), true, "example.com/m")
	assert.Equal(t, changeOutput, c.snapshot().Recent[0].Change, "a client that sends no directory says nothing about a move")

	vocab := churnCauseVocab()
	var found bool
	for _, text := range vocab {
		if text.Key == changePath {
			found = true
			assert.Contains(t, text.Explain, "-trimpath")
		}
	}
	assert.True(t, found, "the vocabulary names the path cause")
}

// One CI run compiles a package for the binary, with its test files, and
// with coverage counters. Each shape has its own key by design. A run that
// stores all of them is no re-store. The next run's change is compared
// with the same shape of the last run.
func TestChurnTracksEachBuildShapeOnItsOwn(t *testing.T) {
	c := testChurn()
	run := func(first byte, content string) {
		plain := pkgMeta(content, "go1.27", "r100", "a.go")
		plain["variant"], plain["src-dir"] = "plain", "example.com/m/p"
		test := pkgMeta(content+"-test", "go1.27", "r100", "a.go a_test.go")
		test["variant"], test["src-dir"] = "test", "/home/runner/work/m/m/p"
		cover := pkgMeta(content+"-cover", "go1.27", "r100", "a.go a.cover.go a_test.go")
		cover["variant"], cover["src-dir"] = "cover+test", "/home/runner/work/m/m/p"
		c.observe(cacheprogKey(first), plain, true, "example.com/m")
		c.observe(cacheprogKey(first+1), test, true, "example.com/m")
		c.observe(cacheprogKey(first+2), cover, true, "example.com/m")
	}
	run('a', "v1")
	s := c.snapshot()
	assert.Zero(t, s.Restores, "three shapes of one package in one run are three packages")
	assert.Equal(t, 3, s.Tracked)

	run('d', "v2")
	s = c.snapshot()
	require.EqualValues(t, 3, s.Restores)
	var shapes []string
	for _, ev := range s.Recent {
		assert.Equal(t, changeOutput, ev.Change, "an edited file is changed content, not changed sources: %s", ev.Variant)
		shapes = append(shapes, ev.Variant)
	}
	assert.ElementsMatch(t, []string{"plain", "test untrimmed", "cover+test untrimmed"}, shapes)
	require.Len(t, s.Top, 3)
	assert.Equal(t, "cover+test untrimmed", s.Top[0].Variant, "equal counts rank by name, so the order is stable")
}

// An upload from a client older than the variant field is read from the
// source names it sent. An absolute directory marks an untrimmed
// compile on either kind of machine.
func TestChurnReadsTheShapeOfAnOlderClient(t *testing.T) {
	shape := func(src, dir string) string {
		return buildShape(map[string]string{"src": src, "src-dir": dir})
	}
	assert.Equal(t, "plain", shape("a.go b.go +3 more", "example.com/m/p"))
	assert.Equal(t, "plain", shape("a.go", "example.com/dep@v1.2.3/p"))
	assert.Equal(t, "test untrimmed", shape("a.go a_test.go", "/home/runner/work/m/m/p"))
	assert.Equal(t, "cover untrimmed", shape("a.go a.cover.go covervars.go", `D:\a\m\m\p`))
	assert.Equal(t, "cover+test untrimmed", shape("a.go covervars.go a_test.go", `\\host\share\p`))
	assert.Equal(t, "cover+test", buildShape(map[string]string{"variant": "cover+test", "src": "a.go"}), "the client's own answer wins")
	assert.Equal(t, "plain", shape("", ""))
}

// A dependency bump changes only the version in the module-cache directory.
// It is named as that, with both versions, and a move to another directory
// is not read as a bump.
func TestChurnNamesADependencyBump(t *testing.T) {
	c := testChurn()
	at := func(dir string) map[string]string {
		meta := pkgMeta("content-"+dir, "go1.27", "r100", "a.go")
		meta["src-dir"] = dir
		return meta
	}
	c.observe(cacheprogKey('a'), at("example.com/dep@v0.0.0-20261121200720-486eb2bb3ae5/cell"), true, "example.com/m")
	c.observe(cacheprogKey('b'), at("example.com/dep@v0.0.0-20261122031822-a85e418f809e/cell"), true, "example.com/m")
	c.observe(cacheprogKey('c'), at("example.com/dep@v0.0.0-20261122031822-a85e418f809e/other"), true, "example.com/m")
	s := c.snapshot()
	require.Len(t, s.Recent, 2)
	assert.Equal(t, changePath, s.Recent[0].Change, "another directory of the same version is a move")
	assert.Equal(t, changeModule, s.Recent[1].Change)
	assert.Equal(t, []fieldDiff{{Field: "src-dir", From: "example.com/dep@v0.0.0-20261121200720-486eb2bb3ae5/cell", To: "example.com/dep@v0.0.0-20261122031822-a85e418f809e/cell"}}, s.Recent[1].Diffs)

	assert.True(t, moduleVersionOnly("example.com/dep@v1.0.0", "example.com/dep@v1.1.0"), "the module root has no directory after the version")
	assert.True(t, moduleVersionOnly("/home/u/go/pkg/mod/example.com/dep@v1.0.0/p", "/home/u/go/pkg/mod/example.com/dep@v1.1.0/p"))
	assert.False(t, moduleVersionOnly("example.com/m/p", "example.com/m/q"), "a directory with no version is not a bump")
	assert.False(t, moduleVersionOnly("example.com/dep@v1.0.0/p", "example.com/m/p"))
	for _, text := range churnCauseVocab() {
		if text.Key == changeModule {
			assert.Contains(t, text.Explain, "module@version")
			return
		}
	}
	t.Fatal("the vocabulary names the dependency bump")
}

// A put of a package the tracker knows under another key is a re-store.
// The field that differs names the change, and an identical output is
// noted beside it.
func TestChurnClassifiesARestoreByWhatDiffered(t *testing.T) {
	c := testChurn()
	c.observe(cacheprogKey('a'), pkgMeta("out1", "go1.27", "r100", "a.go b.go"), true, "example.com/m")
	c.observe(cacheprogKey('a'), pkgMeta("out1", "go1.27", "r100", "a.go b.go"), true, "example.com/m")
	assert.Zero(t, c.snapshot().Restores, "the same key again is not a re-store")

	c.observe(cacheprogKey('b'), pkgMeta("out1", "go1.28", "r100", "a.go b.go"), true, "example.com/m")
	c.observe(cacheprogKey('c'), pkgMeta("out1", "go1.28", "r101", "a.go b.go"), true, "example.com/m")
	c.observe(cacheprogKey('d'), pkgMeta("out2", "go1.28", "r101", "a.go b.go c.go"), true, "example.com/m")
	c.observe(cacheprogKey('e'), pkgMeta("out2", "go1.28", "r101", "a.go b.go c.go"), true, "example.com/m")
	c.observe(cacheprogKey('f'), pkgMeta("out3", "go1.28", "r101", "a.go b.go c.go"), true, "example.com/m")

	s := c.snapshot()
	assert.EqualValues(t, 5, s.Restores)
	assert.EqualValues(t, 3, s.SameOutput, "the Go version, the client and the input changes kept the output")
	byKey := map[string]churnCause{}
	for _, cause := range s.Causes {
		byKey[cause.Key] = cause
	}
	assert.InDelta(t, 1, byKey[changeGoVersion].Count, 0)
	assert.InDelta(t, 1, byKey[changeToolchain].Count, 0)
	assert.InDelta(t, 1, byKey[changeSources].Count, 0)
	assert.InDelta(t, 1, byKey[changeInputs].Count, 0)
	assert.InDelta(t, 1, byKey[changeOutput].Count, 0)
	assert.InDelta(t, 0.2, byKey[changeInputs].Share, 0.001)

	require.Len(t, s.Recent, 5, "newest first")
	assert.Equal(t, changeOutput, s.Recent[0].Change)
	assert.Equal(t, changeInputs, s.Recent[1].Change)
	assert.True(t, s.Recent[1].SameOutput)
	assert.Equal(t, changeGoVersion, s.Recent[4].Change)
	assert.Equal(t, []fieldDiff{{Field: "go-version", From: "go1.27", To: "go1.28"}}, s.Recent[4].Diffs)

	require.Len(t, s.Top, 1)
	assert.Equal(t, "example.com/m/p", s.Top[0].Package)
	assert.EqualValues(t, 5, s.Top[0].Restores)
	assert.EqualValues(t, 3, s.Top[0].SameOutput)
	assert.Equal(t, changeOutput, s.Top[0].LastChange)
	assert.Equal(t, 1, s.Tracked)
}

// A hit teaches the tracker the newest object the cache served, so the next
// put compares against it. A hit is never a re-store itself.
func TestChurnLearnsFromHitsAndCountsOnlyPuts(t *testing.T) {
	c := testChurn()
	c.observe(cacheprogKey('a'), pkgMeta("out1", "go1.27", "r100", "a.go"), false, "example.com/reader")
	c.observe(cacheprogKey('b'), pkgMeta("out1", "go1.27", "r100", "a.go"), false, "example.com/reader")
	assert.Zero(t, c.snapshot().Restores)

	c.observe(cacheprogKey('c'), pkgMeta("out1", "go1.27", "r100", "a.go"), true, "example.com/writer")
	s := c.snapshot()
	assert.EqualValues(t, 1, s.Restores)
	assert.Equal(t, changeInputs, s.Recent[0].Change)
	assert.Equal(t, "example.com/writer", s.Recent[0].Project, "the re-store counts for the build that stored it")
}

// An object with no package, such as a linked binary, is not tracked. A
// main package is not either, because every test main and directory build
// carries that same import path. The same package for another target is
// another package.
func TestChurnIgnoresObjectsWithoutAPackageAndSplitsTargets(t *testing.T) {
	c := testChurn()
	c.observe(cacheprogKey('a'), map[string]string{"outputid": "x"}, true, "p")
	c.observe(cacheprogKey('b'), map[string]string{"outputid": "y"}, true, "p")
	for _, pkg := range []string{"main", "command-line-arguments"} {
		c.observe(cacheprogKey('c'), map[string]string{"outputid": "x", "pkg": pkg}, true, "p")
		c.observe(cacheprogKey('d'), map[string]string{"outputid": "x", "pkg": pkg}, true, "p")
	}
	assert.Zero(t, c.snapshot().Tracked)

	linux := pkgMeta("out1", "go1.27", "r100", "a.go")
	darwin := pkgMeta("out1", "go1.27", "r100", "a.go")
	darwin["target"] = "darwin/arm64"
	c.observe(cacheprogKey('c'), linux, true, "p")
	c.observe(cacheprogKey('d'), darwin, true, "p")
	s := c.snapshot()
	assert.Equal(t, 2, s.Tracked)
	assert.Zero(t, s.Restores, "another target is another package, not a re-store")
}

// The verdict names the dominant change with its newest example, and the
// counters and the project rows move with it.
func TestChurnVerdictAndCounters(t *testing.T) {
	c := testChurn()
	assert.Contains(t, c.snapshot().Verdict, "No package has been stored again")

	const project = "example.com/churn-verdict"
	before := testutil.ToFloat64(packageRestoresTotal.WithLabelValues(changeToolchain, "same"))
	c.observe(cacheprogKey('a'), pkgMeta("out1", "go1.27", "r100", "a.go"), true, project)
	c.observe(cacheprogKey('b'), pkgMeta("out1", "go1.27", "r101", "a.go"), true, project)
	c.observe(cacheprogKey('c'), pkgMeta("out1", "go1.27", "r102", "a.go"), true, project)
	s := c.snapshot()
	assert.Contains(t, s.Verdict, "2 packages were stored again under a new key, 2 of them (100.0%) compiled to the identical content.")
	assert.Contains(t, s.Verdict, "100.0% of the re-stores: client version changed.")
	assert.Contains(t, s.Verdict, "Latest: example.com/m/p toolchain-version r101 -> r102.")
	assert.Equal(t, before+2, testutil.ToFloat64(packageRestoresTotal.WithLabelValues(changeToolchain, "same")))
	row := projectTotals.snapshot()[project]
	assert.EqualValues(t, 2, row.Restores)
	assert.EqualValues(t, 2, row.RestoresSameOutput)
}

// An upload with no content hash cannot answer whether the content changed.
// It is a re-store of unknown output, never a same one, and the verdict
// says so. cmd/go stamps the action id into every archive, so the output id
// alone always differs and must not be read as changed content.
func TestChurnWithoutAContentHashIsUnknownNotChanged(t *testing.T) {
	c := testChurn()
	old := pkgMeta("c1", "go1.27", "r100", "a.go")
	delete(old, "content-id")
	c.observe(cacheprogKey('a'), old, true, "p")
	older := pkgMeta("c2", "go1.27", "r100", "a.go")
	delete(older, "content-id")
	c.observe(cacheprogKey('b'), older, true, "p")
	c.observe(cacheprogKey('c'), pkgMeta("c3", "go1.27", "r100", "a.go"), true, "p")
	c.observe(cacheprogKey('d'), pkgMeta("c3", "go1.27", "r100", "a.go"), true, "p")

	s := c.snapshot()
	assert.EqualValues(t, 3, s.Restores)
	assert.EqualValues(t, 2, s.UnknownOutput, "two re-stores had no content hash on one side")
	assert.EqualValues(t, 1, s.SameOutput)
	assert.Equal(t, outputUnknown, s.Recent[2].Output)
	assert.Equal(t, changeOutput, s.Recent[2].Change)
	assert.Equal(t, outputSame, s.Recent[0].Output)
	assert.Equal(t, changeInputs, s.Recent[0].Change)
	assert.Contains(t, s.Verdict, "and 2 came from a client that sent no content hash")
	assert.NotNil(t, s.Recent[0].Diffs, "a re-store with no differing field still lists an empty set, never null")
}

// The ring keeps the newest examples and the ranking the busiest packages.
func TestChurnBoundsItsExamplesAndRanking(t *testing.T) {
	c := testChurn()
	for i := 0; i < churnTopMax+5; i++ {
		meta := pkgMeta("out", "go1.27", "r100", "a.go")
		meta["pkg"] = "example.com/m/p" + strings.Repeat("x", i)
		c.observe(cacheprogKey('a'), meta, true, "p")
		for j := 0; j <= i%3; j++ {
			c.observe(cacheprogKey(byte('b'+j)), meta, true, "p")
		}
	}
	s := c.snapshot()
	assert.Len(t, s.Recent, churnRecentMax)
	assert.Len(t, s.Top, churnTopMax)
	assert.EqualValues(t, 3, s.Top[0].Restores, "the busiest package ranks first")
	assert.GreaterOrEqual(t, s.Top[0].Restores, s.Top[churnTopMax-1].Restores)
}

// The snapshot the page polls carries the panel, filled by the real put path.
func TestDashboardStatsCarryTheChurnPanel(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	ts, _ := testSetupWithStorage(t)
	hdr := func(content, goVersion string) map[string]string {
		return map[string]string{
			"X-Cache-Meta-Outputid": "o-" + content, "X-Cache-Meta-Content-Id": content, "X-Cache-Meta-Pkg": "example.com/wire/p", "X-Cache-Meta-Target": "linux/amd64",
			"X-Cache-Meta-Go-Version": goVersion, "X-Cache-Meta-Src": "a.go", "X-Cache-Module": "example.com/wire",
		}
	}
	for i, goVersion := range []string{"go1.27", "go1.27", "go1.28"} {
		resp := doRequest(t, ts, "PUT", "/testbucket/"+cacheprogKey(byte('1'+i)), []byte("body"), hdr("same", goVersion))
		require.Equal(t, 200, resp.StatusCode)
		resp.Body.Close()
	}
	d := newDashboard(&Server{}, &Config{Bucket: "testbucket", DataDir: t.TempDir()}, time.Now())
	stats, err := d.snapshot()
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.Churn.Restores)
	assert.EqualValues(t, 2, stats.Churn.SameOutput)
	assert.Equal(t, changeInputs, stats.Churn.Recent[1].Change)
	assert.Equal(t, changeGoVersion, stats.Churn.Recent[0].Change)
	assert.Equal(t, "example.com/wire", stats.Churn.Recent[0].Project)
}

// The page draws the panel the server reports.
func TestDashboardDrawsTheChurnPanel(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard.html")
	require.NoError(t, err)
	script, err := dashboardAssets.ReadFile("dashboard.js")
	require.NoError(t, err)
	assert.Contains(t, string(page), `<h2>WHAT CHANGED</h2>`)
	assert.Contains(t, string(script), `stats.churn`)
	for _, id := range []string{"churn-verdict", "churn-causes", "churn-recent", "churn-top", "churn-label"} {
		assert.Contains(t, string(page), `id="`+id+`"`)
	}
}
