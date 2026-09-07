package cacheclient

import (
	"bytes"
	"io"
	"strconv"
	"strings"

	"github.com/pierrec/lz4/v4"
)

// Compress frames data as lz4, the wire form every stored body takes.
func Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decompress(data []byte) ([]byte, error) {
	return DecompressSized(data, 0)
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
	r := lz4.NewReader(bytes.NewReader(data))
	if size <= 0 || size > maxPresizedBody {
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
