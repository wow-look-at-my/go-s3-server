package cacheclient

import (
	"bytes"
	"io"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// Shims that keep a test's own vocabulary -- a reader, a key string, seven
// return values -- from having to change everywhere the client's internals
// did. Each one calls the real code path; none of them stands in for it.

// hashOf is parseActionHash for a test that knows its id is well formed.
func hashOf(actionID string) actionHash {
	h, _ := parseActionHash(actionID)
	return h
}

// hashOfKey is the same for a full cache key.
func hashOfKey(key string) actionHash {
	h, _ := decodeActionHash(key)
	return h
}

// keySetToHashes converts a set of full cache keys into the hash set the index
// speaks, for a test that builds its expectation as key strings.
func keySetToHashes(keys set.Set[string]) *hashSet {
	s := newHashSet(keys.Len())
	for k := range keys.All() {
		s.Add(hashOfKey(k))
	}
	return s
}

// getTest is Get in the shape the tests were written against.
func (b *WebBackend) getTest(actionID string) (string, io.ReadCloser, int64, time.Time, bool, bool, error) {
	outputID, data, t, miss := b.Get(actionID)
	if miss {
		return "", nil, 0, t, true, false, nil
	}
	return outputID, io.NopCloser(bytes.NewReader(data)), int64(len(data)), t, false, false, nil
}

// getBatchTest is getBatch without the caller having to derive the hash.
func (b *WebBackend) getBatchTest(actionID, key string) (string, io.ReadCloser, int64, time.Time, bool, bool, error) {
	r := b.getBatch(actionID, key, hashOf(actionID))
	if r.miss {
		return "", nil, 0, r.t, true, false, nil
	}
	return r.outputID, io.NopCloser(bytes.NewReader(r.data)), int64(len(r.data)), r.t, false, false, nil
}

// putTest is Put from a reader, and it waits for the object to clear the prep
// pool. Put is asynchronous by design; a test that asserts on what Put decided
// needs a point at which the decision has been made.
func (b *WebBackend) putTest(actionID, outputID string, body io.Reader, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if err := b.Put(actionID, outputID, data); err != nil {
		return err
	}
	b.prep.await()
	return nil
}
