// A Go archive names the compile that made it in its build id.

package main

import (
	"bytes"
	"io"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A toolchain that stamps contentID/contentID into its archives stores bytes
// that a client older than the stamp refuses as a build-id mismatch. That
// client compiles the package again and uploads it with actionID/contentID.
// Both bodies are one compiled object, and only the stamp differs.

// contentStampRepairsTotal counts content-stamped archives the server replaced
// with the same object under its action stamp.
var contentStampRepairsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "s3_content_stamp_repairs_total",
	Help: "Stored archives with a contentID/contentID build id that an upload of the same object with an actionID/contentID build id replaced, so a client older than the content stamp can use the key.",
})

// stampHeadBytes bounds the decompressed bytes read for a build id.
const stampHeadBytes = 4096

// archiveStamp reads the build id of the Go archive in the file at path, as
// its action and content fields. ok is false for a body that is no Go
// archive, or that carries no build id.
func archiveStamp(path string) (action, content string, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", false
	}
	defer f.Close()
	r, release, _, err := decompressingReader(f)
	if err != nil {
		return "", "", false
	}
	defer release()
	head := make([]byte, stampHeadBytes)
	n, _ := io.ReadFull(r, head)
	return parseArchiveStamp(head[:n])
}

// parseArchiveStamp finds `build id "ACTION/CONTENT"` in the export header of
// an archive's leading bytes.
func parseArchiveStamp(head []byte) (action, content string, ok bool) {
	if !bytes.HasPrefix(head, []byte("!<arch>\n")) {
		return "", "", false
	}
	if end := bytes.Index(head, []byte("$$")); end >= 0 {
		head = head[:end]
	}
	const marker = `build id "`
	start := bytes.Index(head, []byte(marker))
	if start < 0 {
		return "", "", false
	}
	rest := head[start+len(marker):]
	end := bytes.IndexByte(rest, '"')
	if end < 0 {
		return "", "", false
	}
	id := string(rest[:end])
	slash := bytes.IndexByte(rest[:end], '/')
	if slash < 0 {
		return "", "", false
	}
	return id[:slash], id[slash+1:], true
}

// replacesContentStamp reports whether the upload at incoming may replace the
// stored body at stored: the stored archive carries a content stamp. The
// upload is the same compiled object with an action stamp.
func replacesContentStamp(stored, incoming string) bool {
	oldAction, oldContent, ok := archiveStamp(stored)
	if !ok || oldAction != oldContent {
		return false
	}
	newAction, newContent, ok := archiveStamp(incoming)
	return ok && newContent == oldContent && newAction != newContent
}
