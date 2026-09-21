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

func TestNoteProjectMissIgnoresANonPositiveCount(t *testing.T) {
	// A batch that found everything asks for no miss to be recorded, and a
	// negative count would otherwise subtract from a counter and panic.
	assert.NotPanics(t, func() {
		noteProjectMiss(requestProvenance{module: "github.com/wow-look-at-my/js-snippets"}, 0)
		noteProjectMiss(requestProvenance{module: "github.com/wow-look-at-my/js-snippets"}, -3)
	})
}
