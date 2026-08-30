package cacheclient

import "github.com/wow-look-at-my/go-containers/set"

// A consumer that layers its own tiers on top of this client has tests that
// drive the key grammar and the read guards without a live remote: a pack
// store's prefetch ordering, a local tier's refusal of a module index. Those
// tests cannot reach unexported fields across a module boundary, and standing
// up an HTTP server to seed one key would test the server instead.

// NewBareBackend returns a backend that talks to no remote and fetches no
// index. Its key grammar and its guards work; Get and Put do not.
func NewBareBackend(prefix string) *WebBackend {
	if prefix == "" {
		prefix = "go-buildcache/"
	}
	return &WebBackend{
		prefix:    prefix,
		keys:      set.New[string](),
		knownMiss: set.New[string](),
	}
}

// MarkPresent records that the remote holds actionID, the same claim a Put
// makes. A Get for a claimed key takes the batch path instead of missing.
func (b *WebBackend) MarkPresent(actionID string) {
	key := b.key(actionID)
	b.keysMu.Lock()
	b.keys.Add(key)
	b.keysMu.Unlock()
}

// Present reports whether actionID is claimed, by the startup index or a Put.
func (b *WebBackend) Present(actionID string) bool {
	return b.keyKnown(b.key(actionID))
}
