package main

import (
	"encoding/binary"
)

// The module-index guard needs the earliest decompressed bytes of a body and
// nothing more.
//
// It is also unnecessary. An lz4 frame's earliest block always begins with a
// literal run (there is no history for a match to reference yet), and literals
// are stored verbatim, so the leading decompressed bytes can be read straight
// out of the frame with no decompression at all: parse the frame header, read
// the earliest block's token, and the literals follow. That is a handful of
// byte loads regardless of object size.
const (
	lz4FrameMagic  = 0x184D2204
	lz4UncompMask  = 0x80000000
	lz4LitLenExtra = 15
)

// lz4LeadingBytes returns up to want leading DECOMPRESSED bytes of the lz4
// frame at the start of head, without decompressing anything.
//
// It reports ok=false for every shape it does not fully understand -- a
// truncated head, a non-frame (skippable or legacy) magic, a version it does
// not know, a dictionary frame (whose earliest block CAN match into the
// dictionary, so its literals are not necessarily the leading bytes), or an
// empty frame. The caller then falls back to a real decode: this is an
// optimization, never another source of truth about what a body is.
//
// The returned slice may be SHORTER than want when the earliest literal run
// is: that is a genuine answer about the leading bytes, and a caller
// comparing against a magic can still decide "no" from a mismatching prefix.
func lz4LeadingBytes(head []byte, want int) ([]byte, bool) {
	p := 0
	left := func() int { return len(head) - p }

	if left() < 7 {
		return nil, false
	}
	if binary.LittleEndian.Uint32(head[p:]) != lz4FrameMagic {
		return nil, false
	}
	p += 4
	flg := head[p]
	p += 2 // FLG, BD
	if flg>>6 != 1 {
		return nil, false
	}
	if flg&0x01 != 0 {
		return nil, false // dictionary: leading bytes may come from the dict
	}
	if flg&0x08 != 0 {
		p += 8 // content size
	}
	p++ // header checksum
	if left() < 4 {
		return nil, false
	}
	blockSize := binary.LittleEndian.Uint32(head[p:])
	p += 4
	if blockSize == 0 {
		return nil, false // EndMark: an empty frame has no leading bytes
	}

	avail := left()
	if blockSize&lz4UncompMask != 0 {
		// Stored uncompressed: the block IS the decompressed bytes.
		if n := int(blockSize &^ lz4UncompMask); avail > n {
			avail = n
		}
		if avail == 0 {
			return nil, false
		}
		return head[p : p+min(avail, want)], true
	}

	if avail < 1 {
		return nil, false
	}
	token := head[p]
	p++
	litLen := int(token >> 4)
	if litLen == lz4LitLenExtra {
		for {
			if left() < 1 {
				return nil, false
			}
			b := head[p]
			p++
			litLen += int(b)
			if b != 255 {
				break
			}
		}
	}
	if litLen == 0 {
		return nil, false // no literal run to read (not valid for a earliest
	}
	avail = left()
	if avail > litLen {
		avail = litLen
	}
	if avail == 0 {
		return nil, false // the head stopped before the literals: cannot tell
	}
	return head[p : p+min(avail, want)], true
}

// lz4HasPrefix reports whether the lz4 frame at the start of head decompresses
// to something beginning with want, when that can be settled from the frame's
// earliest literal run alone. decided=false means the caller must decode for real.
//
// A literal run SHORTER than want still decides the common case: a compiled Go
// object starts "!<arch>\n", which diverges from "go index v" within bytes, so
// the guard answers without decompressing even when the run is tiny.
func lz4HasPrefix(head []byte, want string) (match, decided bool) {
	lead, ok := lz4LeadingBytes(head, len(want))
	if !ok {
		return false, false
	}
	if len(lead) >= len(want) {
		return string(lead[:len(want)]) == want, true
	}
	if want[:len(lead)] != string(lead) {
		return false, true // already diverged: cannot be the prefix
	}
	return false, false // a matching partial run: decode to be sure
}
