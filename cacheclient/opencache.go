// CloseStore drains this process's uploads and gives up the key index's lock.

package cacheclient

import "sync/atomic"

var liveTier atomic.Pointer[storeTier]

// CloseStore drains this process's uploads and gives up the key index's lock.
func CloseStore() error {
	if tier := liveTier.Load(); tier != nil {
		return tier.closeStore()
	}
	return nil
}

// OpenCache answers the cache this process should use, and it is the whole
// decision a consumer makes about caching.
func OpenCache(dir string, store *WebBackend) (Cache, error) {
	if child := dialBroker(dir); child != nil {
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
		// The load and its TLS handshake overlap the go command's package loading.
		go store.ensureIndex()
		// An uploader that died left its spool. The sweep never delays the build.
		if canSpawn && handOffOn.Load() {
			go sweepSpools(store.spoolDir)
		}
	}
	serveBroker(owner, dir)
	return owner, nil
}
