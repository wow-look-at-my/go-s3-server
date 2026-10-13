package main

import "os"

// digestMemoKind is the label the digest memo reports its size under.
const digestMemoKind = "verified-digests"

// digestEntryBytes is what a single memoized verdict costs: the hash, the file identity.
const digestEntryBytes = 120

// fileIdentity names one version of a stored file. A put renames a new
// file over the path, which changes the identity.
type fileIdentity struct {
	ino   uint64
	size  int64
	mtime int64
}

func newDigestMemo(budget int64) *lruCache[cleanKey, fileIdentity] {
	return newLRUCache(budget,
		func(h cleanKey) uint32 {
			return uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16 | uint32(h[3])<<24
		},
		func(cleanKey, fileIdentity) int64 { return digestEntryBytes })
}

func identityOf(f *os.File) (fileIdentity, bool) {
	fi, err := f.Stat()
	if err != nil {
		return fileIdentity{}, false
	}
	return fileIdentity{ino: inodeOf(fi), size: fi.Size(), mtime: fi.ModTime().UnixNano()}, true
}

// verifyStoredDigestFor checks f against its stored digest. A file version
// that already passed skips the hash, but only when the object carries an
// outputid: the client checks the body against it. Rot after the first check
// still never reaches a build. An object with no outputid is hashed on every
// read, because self-heal mints its outputid from the bytes on disk.
func (s *Storage) verifyStoredDigestFor(key string, f *os.File, meta map[string]string) (bool, error) {
	hash, hashed := extractActionHash(key)
	id, known := identityOf(f)
	memo := s.digestOK != nil && hashed && known && meta[outputIDMetaKey] != ""
	if memo {
		if got, ok := s.digestOK.Get(hash); ok && got == id {
			return true, nil
		}
	}
	ok, err := verifyStoredDigest(f, meta)
	if memo && ok && err == nil {
		s.digestOK.Put(hash, id)
	}
	return ok, err
}
