package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// useTestPool returns a pool that brought fetched entries, with the sample due at fetched.
func useTestPool(fetched int) (*lookAhead, []actionHash) {
	la := &lookAhead{brought: set.New[actionHash](), nextJudge: int64(fetched)}
	hashes := make([]actionHash, fetched)
	for idx := range hashes {
		hashes[idx][0] = byte(idx)
		hashes[idx][1] = byte(idx >> 8)
	}
	la.noteBrought(hashes)
	la.Entries.Add(uint32(fetched))
	la.noteSampled(int64(fetched))
	return la, hashes
}

// lookups makes count lookups of keys the pool did not bring.
func lookups(la *lookAhead, count int) {
	for range count {
		la.noteLookup()
	}
}

func TestLookAheadPausesAtTheSample(t *testing.T) {
	la := &lookAhead{brought: set.New[actionHash]()}
	la.noteSampled(lookAheadJudgeAfter - 1)
	require.False(t, la.off(), "a sample not yet full sends on")
	la.noteSampled(1)
	require.True(t, la.off(), "a full sample waits for its verdict")
	require.False(t, la.wasteful.Load(), "the build had no chance to use the sample yet")
}

func TestLookAheadStopsWhenTheBuildUsesLittle(t *testing.T) {
	const sample = 256
	la, hashes := useTestPool(sample)
	for _, h := range hashes[:sample/10] {
		la.noteUsed(h)
	}
	lookups(la, sample/2-sample/10-1)
	require.False(t, la.wasteful.Load(), "the build has not yet made half as many lookups as the sample")
	require.True(t, la.off(), "the pool stays paused until the verdict")
	lookups(la, 1)
	require.True(t, la.wasteful.Load(), "a tenth used is below the floor")
	la.Seed([]string{"k"})
	require.Empty(t, la.seeds, "a stopped pool takes no seed")
}

func TestLookAheadGoesOnWhenTheBuildUsesEnough(t *testing.T) {
	const sample = 256
	floor := sample * lookAheadMinUse / 100
	la, hashes := useTestPool(sample)
	for _, h := range hashes[:floor] {
		la.noteUsed(h)
		la.noteUsed(h)
	}
	require.Equal(t, uint32(floor), la.Used.Load(), "a key counts once")
	require.True(t, la.off(), "one entry short of the floor")
	la.noteUsed(hashes[sample-1])
	require.False(t, la.off(), "the floor reached, the pool goes on")
	require.Equal(t, int64(2*sample), la.nextJudge, "the next verdict comes at twice the sample")
	la.noteSampled(sample - 1)
	require.False(t, la.off())
	la.noteSampled(1)
	require.True(t, la.off(), "the doubled sample waits for its own verdict")
}

func TestLookAheadIgnoresKeysItDidNotBring(t *testing.T) {
	la, _ := useTestPool(4)
	la.noteUsed(actionHash{0xff, 0xff, 0xff})
	require.Zero(t, la.Used.Load())
}
