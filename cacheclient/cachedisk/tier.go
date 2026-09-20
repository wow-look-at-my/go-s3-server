// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cachedisk

// Tier names, as a cache reports them for a lookup it answered.
const (
	TierDisk   = "disk"
	TierShared = "shared"
	TierOwner  = "owner"
)

// Tiered is implemented by a Cache whose lookup reports which tier answered.
// Without it a hit served over the network cannot be told from one served off
// the local disk, and that is the single most useful thing a cache trace says.
type Tiered interface {
	GetTiered(id ActionID) (entry Entry, tier string, err error)
}

// GetTiered answers for the disk cache, which is the only tier it has.
func (c *DiskCache) GetTiered(id ActionID) (Entry, string, error) {
	entry, err := c.Get(id)
	return entry, TierDisk, err
}
