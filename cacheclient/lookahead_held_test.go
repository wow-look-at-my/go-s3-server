package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A body the look-ahead pool received is stated as held in the next request.
// Without that, the server sends the same window again on every request.
func TestLookAheadEntriesAreStatedAsHeld(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "1")
	seedID := testActionID(0x31)
	windowID := testActionID(0x32)
	f := newPrefetchFixture(t, "")

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	b.OnBatchEntries = newSinkTier().sink

	seedKey := f.store(b.KeyPrefix(), seedID, "the seed", testOutputID("the seed"))
	f.extra = f.store(b.KeyPrefix(), windowID, "a body in the window", testOutputID("a body in the window"))
	require.Nil(t, b.heldFilter(), "nothing is held before the window arrives")

	b.lookAhead.expand([]string{seedKey})

	filter := b.heldFilter()
	require.NotNil(t, filter)
	h, ok := parseActionHash(windowID)
	require.True(t, ok)
	require.True(t, filter.contains(h), "a window entry is held once it arrives")
}
