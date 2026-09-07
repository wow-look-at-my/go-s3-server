package cacheclient

import (
	"bytes"
	"encoding/binary"
	"io"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// The wire codec is zstd. It replaced lz4 because the constraint on this cache
// is bandwidth, not compression speed: a build that waits on a link cares how
// many bytes cross it, and zstd carries far fewer for a comparable cost. The
// compression itself runs on the prep pool, off the goroutine that just
// finished a compile, so what it spends is no longer charged to the build.
//
// A stored object names its codec in its own first bytes, so a cache holding
// both is read correctly without consulting metadata, and without a migration.
// The lz4 reader therefore stays for as long as lz4 objects do.
const (
	zstdMagic = 0xFD2FB528
	lz4Magic  = 0x184D2204
)

// zstdEncoder and zstdDecoder are shared. Building either one costs far more
// than a single object, and EncodeAll and DecodeAll are safe for concurrent
// use, which is what the prep and look-ahead pools need.
var (
	zstdEncoder = mustZstdEncoder()
	zstdDecoder = mustZstdDecoder()
)

func mustZstdEncoder() *zstd.Encoder {
	// Concurrency 1: the pools above already decide how many objects are in
	// flight, and a second layer of goroutines per object only competes with
	// them for the same cores.
	e, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstdLevel()),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic("cacheclient: zstd encoder: " + err.Error())
	}
	return e
}

func mustZstdDecoder() *zstd.Decoder {
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic("cacheclient: zstd decoder: " + err.Error())
	}
	return d
}

// zstdLevel picks the encoder level. The default trades a little ratio for a
// lot of speed, which is the right default for a machine that also has to
// compile. A bandwidth-bound deployment can buy a smaller wire.
func zstdLevel() zstd.EncoderLevel {
	switch envInt("GO_TOOLCHAIN_CACHE_ZSTD_LEVEL", 0) {
	case 1:
		return zstd.SpeedFastest
	case 3:
		return zstd.SpeedBetterCompression
	case 4:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedDefault
	}
}

// Compress frames data for the wire.
func Compress(data []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(data, nil), nil
}

func Decompress(data []byte) ([]byte, error) {
	return DecompressSized(data, 0)
}

// codecMagic reads the frame magic a stored body opens with.
func codecMagic(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

// DecompressSized is Decompress when the uncompressed length is already known,
// which it is for anything the cache stores: the uploader records it as
// body-size metadata and every read path carries it back.
//
// io.ReadAll cannot know the answer, so it grows a buffer by repeated
// reallocation and copies the whole object several times on the way. A
// multi-megabyte archive is the common case here, and the copies are pure
// waste when the size was on the wire all along. A wrong size costs nothing
// but the old behavior: the buffer is a starting capacity, not a promise.
func DecompressSized(data []byte, size int64) ([]byte, error) {
	presized := size > 0 && size <= maxPresizedBody

	if codecMagic(data) == zstdMagic {
		if presized {
			return zstdDecoder.DecodeAll(data, make([]byte, 0, size))
		}
		return zstdDecoder.DecodeAll(data, nil)
	}

	// lz4, the codec this cache used to write. An object stored then is still
	// served now.
	r := lz4.NewReader(bytes.NewReader(data))
	if !presized {
		return io.ReadAll(r)
	}
	buf := bytes.NewBuffer(make([]byte, 0, size))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// maxPresizedBody bounds what a declared size may allocate up front. The size
// comes off the wire, so a corrupt or hostile value must not turn one response
// into an out-of-memory kill; past this the reader grows as it always did.
const maxPresizedBody = 1 << 30

// detectObjectType identifies the type of a cache entry from its magic bytes.
func detectObjectType(data []byte) string {
	if len(data) >= 8 && string(data[:8]) == "!<arch>\n" {
		return "go-archive"
	}
	if len(data) >= 4 && data[0] == 0x7f && data[1] == 'E' && data[2] == 'L' && data[3] == 'F' {
		return "elf-binary"
	}
	if len(data) >= 4 {
		// Mach-O, in its wide (little-endian and big-endian) and narrow forms.
		m := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
		switch m {
		case 0xcffaedfe, 0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcafebabe:
			return "macho-binary"
		}
	}
	if len(data) >= 2 && data[0] == 'M' && data[1] == 'Z' {
		return "pe-binary"
	}
	if len(data) >= 4 && data[0] == 0x00 && data[1] == 'g' && data[2] == 'o' && data[3] == '1' {
		return "go-object"
	}
	return "unknown"
}

// DescribeData returns a short human-readable label for a cache entry from
// its raw bytes: object type, optional import path, Go version, and target
// (e.g. "go-archive github.com/foo/bar <goversion> linux/amd64"). Falls back to
// just the object type for binaries and unknown formats.
func DescribeData(data []byte) string {
	objType := detectObjectType(data)
	goVer, target := parseArchiveHeader(data)
	if objType == "go-archive" {
		pkg := parseImportPath(data)
		files := parseSourceFiles(data)
		fileStr := ""
		switch len(files) {
		case 0:
		case 1:
			fileStr = " (" + files[0] + ")"
		default:
			fileStr = " (" + files[0] + " +" + strconv.Itoa(len(files)-1) + ")"
		}
		if pkg != "" && goVer != "" && target != "" {
			return objType + " " + pkg + fileStr + " " + goVer + " " + target
		}
		if pkg != "" {
			return objType + " " + pkg + fileStr
		}
	}
	if goVer != "" && target != "" {
		return objType + " " + goVer + " " + target
	}
	return objType
}

// parseArchiveHeader scans a Go archive for the "go object" line inside
// __.PKGDEF. Returns Go version and target (GOOS/GOARCH), or empty strings
// if not found. Only scans the leading bytes.
func parseArchiveHeader(data []byte) (goVersion, target string) {
	limit := 1024
	if len(data) < limit {
		limit = len(data)
	}
	window := data[:limit]
	// Look for a line starting with "go object ".
	const prefix = "go object "
	for len(window) > 0 {
		idx := bytes.Index(window, []byte(prefix))
		if idx < 0 {
			break
		}
		// Ensure it's at the start of a line (at the head, or preceded by a newline).
		if idx > 0 && window[idx-1] != '\n' {
			window = window[idx+len(prefix):]
			continue
		}
		line := window[idx:]
		if nl := bytes.IndexByte(line, '\n'); nl >= 0 {
			line = line[:nl]
		}
		// Format: "go object <GOOS> <GOARCH> <goversion> [experiments...]"
		fields := strings.Fields(string(line))
		if len(fields) >= 5 {
			return fields[4], fields[2] + "/" + fields[3]
		}
		break
	}
	return "", ""
}
