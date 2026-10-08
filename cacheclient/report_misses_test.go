package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReportNamesTheMisses: the routine report named what was cached and said
// nothing about the lookups that missed, so a CI log could not answer whether
// the cache was working at all. The misses it already counted are in the line.
func TestReportNamesTheMisses(t *testing.T) {
	store := &WebBackend{}
	store.Stats.Hits.Store(7)
	store.MissNotInIndex.Store(3)
	store.MissHTTP404.Store(1)

	var msgs []string
	SetLogger(recordingLogger{msgs: &msgs})
	defer SetLogger(nil)

	(&storeTier{store: store}).report()

	require.Len(t, msgs, 1)
	require.Contains(t, msgs[0], ", 4 missed")
	require.Contains(t, msgs[0], "not_in_index=3")
	require.Contains(t, msgs[0], "http_404=1")
	require.Contains(t, msgs[0], "server 7 (")
}

// TestNoticeIsAlwaysReported: the notices were behind GOCACHEDEBUG, so a CI
// log said nothing about the cache unless that variable was set. They reach the
// installed logger now.
func TestNoticeIsAlwaysReported(t *testing.T) {
	var msgs []string
	SetLogger(recordingLogger{msgs: &msgs})
	defer SetLogger(nil)

	cacheNotice("cache: %s", "always")

	require.Equal(t, []string{"cache: always"}, msgs)
}
