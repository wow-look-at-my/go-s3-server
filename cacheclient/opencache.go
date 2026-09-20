package cacheclient

// OpenCache answers the cache this process should use, and it is the whole
// decision a consumer makes about caching.
//
// The first process of a build opens the directory, layers the store under it
// when one is configured, and serves the result to every process it starts.
// Each of those asks it instead: no directory of its own, no key index of its
// own, and no connection to the store. That is why the disk cache, the broker
// and the store client are one module -- a consumer holding a Cache should not
// have to know which of the three answered.
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
		owner = layerStore(disk, store)
	}
	serveBroker(owner, dir)
	return owner, nil
}
