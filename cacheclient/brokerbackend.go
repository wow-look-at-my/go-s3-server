package cacheclient

import "time"

// brokerBackend is the cache as a child process sees it: every call goes to the
// broker over the socket, and this process never dials the store, never loads
// the index, and never holds an upload open at exit.
type brokerBackend struct {
	link   *brokerLink
	prefix string
	stats  CacheStats
}

// newBrokerBackend builds the child's cache. It keeps the key prefix because
// ActionIDFromKey is string work this process can do without asking anybody.
func newBrokerBackend(link *brokerLink, cfg WebConfig) *brokerBackend {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "go-buildcache/"
	}
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return &brokerBackend{link: link, prefix: prefix + "v1"}
}

func (bac *brokerBackend) Get(actionID string) (string, []byte, time.Time, bool) {
	outputID, data, stamp, miss := bac.link.get(actionID)
	if miss {
		return "", nil, time.Time{}, true
	}
	bac.stats.Hits.Increment()
	bac.stats.HitBytes.Add(uint64(len(data)))
	return outputID, data, stamp, false
}

func (bac *brokerBackend) PutFile(actionID, outputID, path string) error {
	if err := bac.link.putFile(actionID, outputID, path); err != nil {
		return err
	}
	bac.stats.Puts.Increment()
	return nil
}

// Verify has nothing to check. A look-ahead entry reaches the process that owns
// the pool, and this one does not have it.
func (bac *brokerBackend) Verify(BatchEntry, string) ([]byte, bool) { return nil, false }

// ActionIDFromKey recovers the action a key names, under the same grammar the
// direct backend uses.
func (bac *brokerBackend) ActionIDFromKey(key string) (string, bool) {
	if len(key) != len(bac.prefix)+2*hashSize || key[:len(bac.prefix)] != bac.prefix {
		return "", false
	}
	return key[len(bac.prefix):], true
}

// SetModule has nothing to stamp here. The requests carrying this process's
// objects are signed by the broker, so the module the root build named is the
// one the store records.
func (bac *brokerBackend) SetModule(string) {}

// SetBatchSink has nothing to feed. The look-ahead pool belongs to the broker,
// and what it fetches lands in the cache directory this process reads.
func (bac *brokerBackend) SetBatchSink(func([]BatchEntry)) {}

// SummarySnapshot reports what this process moved through the broker. The
// index costs nothing here, which is the point of the broker.
func (bac *brokerBackend) SummarySnapshot() WebSummary {
	return WebSummary{
		Hits:     bac.stats.Hits.Load(),
		Puts:     bac.stats.Puts.Load(),
		HitBytes: bac.stats.HitBytes.Load(),
		PutBytes: bac.stats.PutBytes.Load(),
	}
}

// Close drops the sockets. The broker owns every upload this process offered,
// so there is nothing to wait for.
func (bac *brokerBackend) Close() error {
	bac.link.close()
	return nil
}
