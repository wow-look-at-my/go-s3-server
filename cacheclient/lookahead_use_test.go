package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

func useTestPool(fetched int) (*lookAhead, []actionHash) {
	la := &lookAhead{brought: set.New[actionHash]()}
	hashes := make([]actionHash, fetched)
	for idx := range hashes {
		hashes[idx][0] = byte(idx)
		hashes[idx][1] = byte(idx >> 8)
	}
	la.noteBrought(hashes)
	la.Entries.Add(uint32(fetched))
	return la, hashes
}

func TestLookAheadStopsWhenTheBuildUsesLittle(t *testing.T) {
	la, hashes := useTestPool(lookAheadJudgeAfter)
	for _, h := range hashes[:lookAheadJudgeAfter/10] {
		la.noteUsed(h)
	}
	la.judge()
	require.True(t, la.off(), "a tenth used is below the floor")
	la.Seed([]string{"k"})
	require.Empty(t, la.seeds, "a stopped pool takes no seed")
}

func TestLookAheadGoesOnWhenTheBuildUsesEnough(t *testing.T) {
	la, hashes := useTestPool(lookAheadJudgeAfter)
	for _, h := range hashes[:lookAheadJudgeAfter/2] {
		la.noteUsed(h)
		la.noteUsed(h)
	}
	require.Equal(t, uint32(lookAheadJudgeAfter/2), la.Used.Load(), "a key counts once")
	la.judge()
	require.False(t, la.off())
}

func TestLookAheadIsNotJudgedEarly(t *testing.T) {
	la, _ := useTestPool(lookAheadJudgeAfter - 1)
	la.judge()
	require.False(t, la.off(), "too few entries to judge")
}

func TestLookAheadIgnoresKeysItDidNotBring(t *testing.T) {
	la, _ := useTestPool(4)
	la.noteUsed(actionHash{0xff, 0xff, 0xff})
	require.Zero(t, la.Used.Load())
}
