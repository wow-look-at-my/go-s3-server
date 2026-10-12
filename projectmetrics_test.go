package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjectLabelKeepsAKnownName(t *testing.T) {
	p := newProjectLabeller()
	require.Equal(t, "github.com/wow-look-at-my/go-toolchain", p.label("github.com/wow-look-at-my/go-toolchain"))
	require.Equal(t, "github.com/wow-look-at-my/go-toolchain", p.label("  github.com/wow-look-at-my/go-toolchain  "),
		"surrounding space is not a different project")
}

func TestProjectLabelNamesTheAbsentCaseUnknown(t *testing.T) {
	p := newProjectLabeller()
	assert.Equal(t, unknownProject, p.label(""))
	assert.Equal(t, unknownProject, p.label("   "))
}

func TestProjectLabelFoldsPastTheBound(t *testing.T) {
	p := newProjectLabeller()
	for i := range maxProjectLabels {
		name := "project-" + string(rune('a'+i%26)) + strings.Repeat("x", i)
		require.Equal(t, name, p.label(name), "the first %d names each keep their own series", maxProjectLabels)
	}
	assert.Equal(t, otherProject, p.label("one-too-many"), "a new name past the bound folds")
	assert.Equal(t, "project-a", p.label("project-a"), "an already-admitted name keeps its series")
}

func TestProjectLabelRefusesAnImplausibleName(t *testing.T) {
	p := newProjectLabeller()
	assert.Equal(t, otherProject, p.label(strings.Repeat("a", maxProjectNameLen+1)), "too long to be a module path")
	assert.Equal(t, otherProject, p.label("has a space"), "a module path carries no space")
	assert.Equal(t, otherProject, p.label("line\nbreak"))
}

func TestPlausibleProjectName(t *testing.T) {
	assert.True(t, plausibleProjectName("github.com/wow-look-at-my/go-s3-server"))
	assert.True(t, plausibleProjectName("a"))
	assert.False(t, plausibleProjectName("with\ttab"))
	assert.False(t, plausibleProjectName("with\x7fdel"))
}

// The Prometheus label folds a name past the bound into "other". The exact
// totals the dashboard reads must not.
func TestExactProjectsKeepANamePastTheLabelBound(t *testing.T) {
	exact := &exactProjects{rows: map[string]*projectRow{}}
	for idx := range maxProjectLabels + 10 {
		exact.add("example.com/exact-"+strings.Repeat("x", idx), objKindHit, 1, 100)
	}
	last := "example.com/exact-" + strings.Repeat("x", maxProjectLabels+9)
	exact.add(last, objKindMiss, 3, 0)

	rows := exact.snapshot()
	require.Len(t, rows, maxProjectLabels+10, "every name keeps its own row")
	assert.Equal(t, projectRow{Hit: 1, Miss: 3, HitBytes: 100}, rows[last])
	assert.NotContains(t, rows, otherProject)
}

func TestExactProjectsCountEachKind(t *testing.T) {
	exact := &exactProjects{rows: map[string]*projectRow{}}
	exact.add("m", objKindPut, 1, 50)
	exact.add("m", objKindLookAhead, 2, 70)
	exact.add("", objKindMiss, 1, 0)
	rows := exact.snapshot()
	assert.Equal(t, projectRow{Put: 1, PutBytes: 50, LookAhead: 2, LookAheadBytes: 70}, rows["m"])
	assert.Equal(t, projectRow{Miss: 1}, rows[unknownProject], "a request with no module counts under unknown")
}

func TestNoteProjectMissIgnoresANonPositiveCount(t *testing.T) {
	// A batch that found everything asks for no miss to be recorded, and a
	// negative count would otherwise subtract from a counter and panic.
	assert.NotPanics(t, func() {
		noteProjectMiss(requestProvenance{module: "github.com/wow-look-at-my/js-snippets"}, 0)
		noteProjectMiss(requestProvenance{module: "github.com/wow-look-at-my/js-snippets"}, -3)
	})
}
