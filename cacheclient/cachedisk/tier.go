// All rights reserved. Use of this source code is
// governed by a BSD-style license that can be found
// in the LICENSE file.

package cachedisk

// Tier names, as a cache reports them for a lookup it answered.
const (
	TierDisk   = "disk"
	TierShared = "shared"
	TierOwner  = "owner"
)

// Tiered is implemented by a Cache whose lookup names the tier that answered.
// That is the most useful thing a cache trace says.
type Tiered interface {
	GetTiered(id ActionID) (entry Entry, tier string, err error)
}

// Layered is implemented by a Cache that puts a shared tier over a directory.
// Local answers the directory alone.
type Layered interface {
	Local() Cache
}

// Local answers the part of c that stays on this machine.
func Local(c Cache) Cache {
	if layered, ok := c.(Layered); ok {
		return layered.Local()
	}
	return c
}

// GetTiered answers for the disk cache, which is the only tier it has.
func (c *DiskCache) GetTiered(id ActionID) (Entry, string, error) {
	entry, err := c.Get(id)
	return entry, TierDisk, err
}
