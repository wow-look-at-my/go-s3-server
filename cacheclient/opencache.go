package cacheclient

import "sync/atomic"

var liveTier atomic.Pointer[storeTier]

// CloseStore drains this process's uploads and gives up the key index's lock.
// The directory stays open, so a caller may still read and write it.
//
// A consumer calls it as the process exits. Every go command opens the cache,
// and a single that exits holding the index's lock makes the next wait for
// it. A command that never runs a build therefore has to give it up here,
// because nothing else will call Close.
func CloseStore() error {
	if tier := liveTier.Load(); tier != nil {
		return tier.closeStore()
	}
	return nil
}

// OpenCache answers the cache this process should use, and it is the whole
// decision a consumer makes about caching.
//
// The earliest process of a build opens the directory, layers the store under
// it when a single is configured, and serves the result to every process it
// starts. Each of those asks it instead: no directory of its own, no key index
// of its own, and no connection to the store. That is why the disk cache, the
// broker and the store client are a single module -- a consumer holding a
// Cache should not have to know which of each answered.
//
// A child that cannot reach the owner opens the directory itself. A slower
// build is the cost of that; a build that stops is not.
func OpenCache(dir string, store *WebBackend) (Cache, error) {
	if child := dialBroker(); child != nil {
		return child, nil
	}
	disk, err := Open(dir)
	if err != nil {
		return nil, err
	}
	var owner Cache = disk
	if store != nil {
		tier := layerStore(disk, store)
		liveTier.Store(tier)
		owner = tier
	}
	serveBroker(owner, dir)
	return owner, nil
}
