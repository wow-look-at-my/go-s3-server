package cacheclient

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The budget is meant to be what one pool may hold at once. It is sampled once,
// before a worker issues its fetch, and the bodies are charged afterwards as the
// response streams. So every worker can pass an empty-pool check together and
// then each hold a chunk, and the pool's real peak is the worker count times the
// chunk rather than the budget.
//
// This drives the real pool at a budget far below one chunk and records what it
// was holding each time it handed bodies over.
func TestLookAheadPeakStaysInsideItsBudget(t *testing.T) {
	const (
		entries   = 64
		entrySize = 256 << 10 // one chunk of 16 is 4 MB
		budget    = 1 << 20   // far below a single chunk
		workers   = 8
	)

	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD_BYTES", fmt.Sprint(budget))
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", fmt.Sprint(workers))

	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	// One key the build asks for, and a window of large bodies behind it. The
	// bodies must be INCOMPRESSIBLE: charge counts the stored bytes, and lz4
	// takes a repeating pattern down to nothing, which measures no pressure at
	// all.
	seedKey := "go-buildcache/v1aaaa000000000000"
	body := make([]byte, entrySize)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range body {
		body[i] = byte(rng.Uint32())
	}
	add := func(key string, payload []byte) {
		compressed, err := Compress(payload)
		require.NoError(t, err)
		store[key] = compressed
		meta[key] = map[string]string{"outputid": testOutputID(string(payload))}
	}
	add(seedKey, []byte("seed"))
	for i := range entries {
		add(fmt.Sprintf("go-buildcache/v1bbbb%059x", i), append([]byte(fmt.Sprint(i)), body...))
	}

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	var mu sync.Mutex
	var peak int64
	b.OnBatchEntries = func(es []BatchEntry) {
		// charge ran just before this, so the counter now names what the pool
		// holds, this chunk included.
		held := b.lookAhead.held.Load()
		mu.Lock()
		if held > peak {
			peak = held
		}
		mu.Unlock()
	}

	_, _, _, _, _, _, err = b.getBatchTest("aaaa000000000000", seedKey)
	require.NoError(t, err)
	require.NoError(t, b.Close())

	mu.Lock()
	defer mu.Unlock()
	require.NotZero(t, peak, "the pool never held anything, so this measured nothing")
	assert.LessOrEqual(t, peak, int64(budget),
		"the pool peaked at %d bytes against a %d byte budget: the budget gates starting a fetch, not holding the result",
		peak, budget)
}
