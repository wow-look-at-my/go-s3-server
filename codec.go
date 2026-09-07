package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// A stored body names its own codec in its first four bytes. The server reads
// that rather than the client's `compression` metadata, because the bytes
// cannot disagree with themselves: an object whose metadata was lost, or
// stamped by a client of another version, is still decoded correctly.
//
// The cache holds both codecs and will for a long time. zstd is what the client
// writes now, because this cache's constraint is bandwidth. lz4 is what it
// wrote before, and those objects stay readable.
const (
	zstdFrameMagic = 0xFD2FB528
	lz4FrameMagicV = 0x184D2204
)

// codecPeekBytes is how many leading bytes settle the codec question.
const codecPeekBytes = 4

// errNoZstdDecoder means the pool could not build one, which only a broken
// option set causes. It is reported rather than swallowed.
var errNoZstdDecoder = errors.New("codec: cannot build a zstd decoder")

// frameCodec names the codec a stored body opens with, or "" when the bytes
// are too short or match neither -- which is how an uncompressed body reads.
func frameCodec(head []byte) string {
	if len(head) < codecPeekBytes {
		return ""
	}
	switch binary.LittleEndian.Uint32(head) {
	case zstdFrameMagic:
		return "zstd"
	case lz4FrameMagicV:
		return "lz4"
	}
	return ""
}

// zstdProbeDecoders reuses decoders across probes. Building one costs far more
// than the sixteen bytes a probe reads, and the read paths probe constantly.
var zstdProbeDecoders = sync.Pool{New: func() any {
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(1<<27))
	if err != nil {
		return nil
	}
	return d
}}

// decompressingReader wraps r in the decoder its own frame magic names. It
// returns the reader to read decompressed bytes from, a release function the
// caller must call, and whether the body was compressed at all.
//
// The peek is non-destructive: whatever it consumed is replayed in front of
// the rest, so the caller hands over a plain io.Reader and gets one back.
func decompressingReader(r io.Reader) (io.Reader, func(), error) {
	br := bufio.NewReaderSize(r, 4096)
	head, err := br.Peek(codecPeekBytes)
	if err != nil && len(head) < codecPeekBytes {
		// Too short to be either frame. Hand back what there is; a caller
		// looking for a magic prefix will simply not find one.
		return br, func() {}, nil
	}

	switch frameCodec(head) {
	case "zstd":
		d, _ := zstdProbeDecoders.Get().(*zstd.Decoder)
		if d == nil {
			return nil, func() {}, errNoZstdDecoder
		}
		if err := d.Reset(br); err != nil {
			zstdProbeDecoders.Put(d)
			return nil, func() {}, err
		}
		return d.IOReadCloser(), func() {
			// Reset to nil releases the decoder's buffers before it goes back
			// to the pool, so an abandoned probe does not park them there.
			_ = d.Reset(nil)
			zstdProbeDecoders.Put(d)
		}, nil
	case "lz4":
		zr := lz4.NewReader(br)
		return zr, func() { zr.Reset(nil) }, nil
	}
	return br, func() {}, nil
}
