package cacheclient

import "sync/atomic"

// The blocking batch path's speculative half.
//
// A /_batch/get response can carry entries beyond the keys the request named:
// the server's prefetch window, the objects stored around them. Nobody in the
// batch is waiting for those, so the request loop has nothing to hand them to.
// Reading them off the wire and returning cost the server a disk read and the
// client its share of the transfer, for nothing at all.
//
// They go to OnBatchEntries instead, the sink the look-ahead pool already
// hands its windows to, which is the consumer's local tier. What arrives there
// is a body the build has not asked for yet and may well ask for next.
//
// Two things keep that from charging the critical path. The hand-off runs on
// its own goroutine, after every caller in the batch has its answer, so no
// build goroutine waits on a decompress for a body it did not want. And the
// bytes held between reading an entry and handing it over are bounded by
// prefetchBudget, because a response's window is whatever the server chose to
// send and a count of entries is not a size.

// prefetchBudget bounds the bytes of unrequested bodies a backend holds at
// once. A limit of zero or less is unbounded, which is what a directly
// constructed backend, asking for no bound, gets.
type prefetchBudget struct {
	held  atomic.Int64
	limit int64
}

// take reserves n bytes and reports whether they fit. A refusal reserves
// nothing.
func (p *prefetchBudget) take(n int64) bool {
	if p.limit <= 0 {
		return true
	}
	if p.held.Add(n) > p.limit {
		p.held.Add(-n)
		return false
	}
	return true
}

// release returns n bytes taken earlier.
func (p *prefetchBudget) release(n int64) {
	if p.limit <= 0 {
		return
	}
	p.held.Add(-n)
}

// prefetchSink holds the unrequested entries of ONE batch response until the
// batch has answered everybody it owes.
type prefetchSink struct {
	b       *WebBackend
	entries []BatchEntry
	held    int64
}

// collect takes an entry nobody in this batch asked for. An entry the sink
// cannot keep is dropped rather than queued: the key stays in the index, so a
// later hit asks for it by name.
func (s *prefetchSink) collect(e BatchEntry) {
	s.b.PrefetchOffered.Increment()
	if s.b.OnBatchEntries == nil {
		return // nowhere to put it; buffering it would be a leak with no reader
	}
	n := int64(len(e.Data))
	if !s.b.prefetchHold.take(n) {
		return
	}
	s.entries = append(s.entries, e)
	s.held += n
}

// deliver hands what this batch collected to the consumer, off the goroutine
// the build is blocked on.
func (s *prefetchSink) deliver() {
	if len(s.entries) == 0 {
		return
	}
	entries, held := s.entries, s.held
	s.entries, s.held = nil, 0
	b := s.b
	b.prefetchWG.Add(1)
	go func() {
		defer b.prefetchWG.Done()
		defer b.prefetchHold.release(held)
		b.storePrefetched(entries)
	}()
}

// storePrefetched puts each entry through the gates a requested body passes
// and hands the survivors over in chunks. A body that fails one is dropped
// here, so nothing the consumer stores is unverified.
//
// The chunking is the look-ahead pool's, for the same reason: the consumer
// sees a bounded slice rather than whatever the server chose to send.
func (b *WebBackend) storePrefetched(entries []BatchEntry) {
	keep := make([]BatchEntry, 0, lookAheadChunk)
	flush := func() {
		if len(keep) == 0 {
			return
		}
		b.OnBatchEntries(keep)
		b.PrefetchStored.Add(uint32(len(keep)))
		keep = make([]BatchEntry, 0, lookAheadChunk)
	}
	for _, e := range entries {
		actionID, ok := b.ActionIDFromKey(e.Key)
		if !ok {
			// No action ID means no gate can be applied to it at all.
			continue
		}
		if _, ok := b.verify("web batch prefetch", actionID, e.OutputID, e.Data, e.RawSize); !ok {
			continue
		}
		keep = append(keep, e)
		if len(keep) >= lookAheadChunk {
			flush()
		}
	}
	flush()
}
