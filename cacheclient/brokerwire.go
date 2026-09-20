// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"
)

// The broker's records. A request and a reply are each one record in a
// shared-memory ring: an opcode in the record's type word, a correlation ID in
// the first eight bytes, and fixed-width fields after it.
//
// No body ever travels. Both processes can open the cache directory, so an
// answer names a file rather than carrying it.
const (
	opHello = 1 // a child names the channel it created
	opGet   = 2 // action ID -> entry
	opPut   = 3 // action ID and a path -> entry
)

// The reply's type word. A miss is a status rather than an error, because it
// is the ordinary answer to most lookups.
const (
	stDir   = 1 // the owner's directory, answered to a hello
	stEntry = 2
	stMiss  = 3
	stError = 4
)

// idBytes is the correlation ID every record starts with.
const idBytes = 8

// entryBytes is a reply's fixed part: the output ID, the size and the time.
const entryBytes = cachedisk.HashSize + 8 + 8

// errShortRecord says a record is too small to hold what its opcode needs. A
// peer that sends one is not this version of the protocol.
var errShortRecord = errors.New("cache broker: truncated record")

// putRecord writes a store request: the correlation ID, the action, and the
// path the body sits at.
func putRecord(dst []byte, id uint64, action ActionID, path string) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, id)
	dst = append(dst, action[:]...)
	return append(dst, path...)
}

// getRecord writes a lookup request.
func getRecord(dst []byte, id uint64, action ActionID) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, id)
	return append(dst, action[:]...)
}

// readRequest reads the correlation ID and action a request carries, and
// whatever follows them.
func readRequest(rec []byte) (id uint64, action ActionID, rest []byte, err error) {
	if len(rec) < idBytes+len(action) {
		return 0, action, nil, errShortRecord
	}
	id = binary.LittleEndian.Uint64(rec)
	copy(action[:], rec[idBytes:])
	return id, action, rec[idBytes+len(action):], nil
}

// entryRecord writes a reply that carries an entry.
func entryRecord(dst []byte, id uint64, entry Entry, tier string) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, id)
	dst = append(dst, entry.OutputID[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(entry.Size))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(entry.Time.UnixNano()))
	return append(dst, tier...)
}

// readEntryRecord reads a reply that carries an entry.
func readEntryRecord(rec []byte) (Entry, string, error) {
	if len(rec) < idBytes+entryBytes {
		return Entry{}, "", errShortRecord
	}
	body := rec[idBytes:]
	var entry Entry
	copy(entry.OutputID[:], body)
	body = body[len(entry.OutputID):]
	entry.Size = int64(binary.LittleEndian.Uint64(body))
	entry.Time = time.Unix(0, int64(binary.LittleEndian.Uint64(body[8:])))
	return entry, string(body[16:]), nil
}

// recordID reads the correlation ID a record starts with.
func recordID(rec []byte) (uint64, error) {
	if len(rec) < idBytes {
		return 0, errShortRecord
	}
	return binary.LittleEndian.Uint64(rec), nil
}
