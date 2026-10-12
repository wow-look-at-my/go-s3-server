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

// A stored body names its own codec in its earliest bytes.
const (
	zstdFrameMagic = 0xFD2FB528
	lz4FrameMagicV = 0x184D2204
)

// codecPeekBytes is how many leading bytes settle the codec question.
const codecPeekBytes = 4

// It is reported rather than swallowed.
var errNoZstdDecoder = errors.New("codec: cannot build a zstd decoder")

// frameCodec names the codec a stored body opens with, or "" when the bytes
// are too short. Otherwise, match neither -- which is how an uncompressed
// body reads.
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

// zstdProbeDecoders reuses decoders across probes.
var zstdProbeDecoders = sync.Pool{New: func() any {
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(1<<27))
	if err != nil {
		return nil
	}
	return d
}}

// decompressingReader wraps r in the decoder its own frame magic names. It
// returns the reader to read decompressed bytes from, a release function the
// caller must call, and the codec it recognized.
//
// The codec is "" for a body that opens with neither frame magic. That reader
// is the bytes as they stand, which is right for a caller matching a magic
// prefix on a possibly-uncompressed body. It is WRONG for a caller that must
// have the decompressed bytes, so such a caller checks the codec. Hashing a
// body that never decompressed would mint a confident, wrong content address.
func decompressingReader(r io.Reader) (io.Reader, func(), string, error) {
	br := bufio.NewReaderSize(r, 4096)
	head, err := br.Peek(codecPeekBytes)
	if err != nil && len(head) < codecPeekBytes {
		// Too short to be either frame.
		return br, func() {}, "", nil
	}

	switch codec := frameCodec(head); codec {
	case "zstd":
		d, _ := zstdProbeDecoders.Get().(*zstd.Decoder)
		if d == nil {
			return nil, func() {}, codec, errNoZstdDecoder
		}
		if err := d.Reset(br); err != nil {
			zstdProbeDecoders.Put(d)
			return nil, func() {}, codec, err
		}
		return d.IOReadCloser(), func() {
			// Reset to nil releases the decoder's buffers before it goes back to the pool.
			_ = d.Reset(nil)
			zstdProbeDecoders.Put(d)
		}, codec, nil
	case "lz4":
		zr := lz4.NewReader(br)
		return zr, func() { zr.Reset(nil) }, codec, nil
	}
	return br, func() {}, "", nil
}
