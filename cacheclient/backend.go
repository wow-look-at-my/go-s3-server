package cacheclient

import "time"

// A consumer asks for a cache, not for a way of reaching one. There are two
// ways: this process dials the store, or it asks the broker a process above it
// runs. Both answer the same calls, so the consumer holds a Backend and never
// learns which one it got.
type Backend interface {
	// Get answers an object, or reports a miss.
	Get(actionID string) (outputID string, data []byte, stamp time.Time, miss bool)

	// PutFile offers a body that is on disk. The path must be readable by
	// whoever ends up uploading it, which is this process or the broker.
	PutFile(actionID, outputID, path string) error

	// Verify checks a look-ahead entry's body against its key.
	Verify(entry BatchEntry, actionID string) ([]byte, bool)

	// ActionIDFromKey recovers the action a cache key names.
	ActionIDFromKey(key string) (string, bool)

	// SetModule names the main module this build is building. The value rides
	// every later request as HeaderModule, and lands in the metadata of every
	// object this process uploads, so the store's log says which build sent
	// the bytes. A go command opens its cache before the module loader has
	// read go.mod, which is why this is a setter and not a config field.
	//
	// A child under a broker stamps nothing: its requests are made by the
	// broker, under the module the root build named.
	SetModule(path string)

	// SetBatchSink installs the sink for look-ahead entries. A backend with no
	// look-ahead of its own ignores it.
	SetBatchSink(sink func(entries []BatchEntry))

	// SummarySnapshot reports what this process's cache did.
	SummarySnapshot() WebSummary

	// Close releases the backend. A direct one drains its uploads here; a
	// broker's child has none to drain.
	Close() error
}

// NewBackend answers the cache this process should use. It asks the broker
// named in the environment first. With none to ask, it dials the store itself
// and becomes the broker for every process it starts.
//
// A nil answer means the configuration names no cache, which is the ordinary
// case on a developer's machine.
func NewBackend(cfg WebConfig) (Backend, error) {
	if cfg.Bucket == "" {
		return nil, nil
	}
	if link := brokerFromEnv(); link != nil {
		return newBrokerBackend(link, cfg), nil
	}
	direct, err := NewWebBackend(cfg)
	if err != nil || direct == nil {
		return nil, err
	}
	direct.broker = startBroker(direct)
	return direct, nil
}

// SetBatchSink installs the look-ahead sink. The field it writes is read by the
// pool, so a consumer that leaves it nil turns look-ahead off.
func (b *WebBackend) SetBatchSink(sink func(entries []BatchEntry)) {
	b.OnBatchEntries = sink
}
