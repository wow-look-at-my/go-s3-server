//go:build js || wasip1

// broker_inproc.go holds the broker inproc code.

package cacheclient

// js and wasip1 start no other process, so no child can ask for the cache.

func serveBroker(Cache, string) {}

func dialBroker(string) Cache { return nil }

// BrokerEnviron is empty: this process serves no cache to another process.
func BrokerEnviron() []string { return nil }

// StopBroker has no broker to stop.
func StopBroker() {}

// Exit holds no endpoint to close.
func Exit() error { return nil }
