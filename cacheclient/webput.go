// putReq is a prepped object queued for the PUT coalescer.

package cacheclient

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// putReq is a prepped object queued for the PUT coalescer.
type putReq struct {
	actionID   string
	key        string
	hash       actionHash
	outputID   string
	compressed []byte            // lz4-compressed body, the data/<key> member bytes
	metadata   map[string]string // manifest metadata: lowercased meta names sans X-Cache-Meta-
}

// Put stores a cached object, compressed, and returns at the same time: the
// guards, the compression and the upload all happen behind it.
func (b *WebBackend) Put(actionID, outputID string, data []byte) error {
	return b.enqueuePut(actionID, outputID, data, "")
}

// PutFile is Put over a body that is still on disk, for a caller holding a
// file rather than bytes.
func (b *WebBackend) PutFile(actionID, outputID, path string) error {
	if path == "" {
		return fmt.Errorf("web put %s: no path to read the body from", ShortID(actionID))
	}
	return b.enqueuePut(actionID, outputID, nil, path)
}

// enqueuePut claims the key and hands the object to a prep worker. The claim is
// the thing thing that happens on the caller's goroutine, because it is what
// stops callers uploading the same object.
func (b *WebBackend) enqueuePut(actionID, outputID string, data []byte, path string) error {
	b.manifest.note(actionID, "put")
	h, ok := parseActionHash(actionID)
	if !ok {
		return fmt.Errorf("web put: %q is not an action ID", actionID)
	}

	// Atomically check-and-claim: skip if the key is already known or being uploaded.
	b.ensureIndex()
	b.keysMu.Lock()
	if b.keys.Contains(h) {
		b.keysMu.Unlock()
		b.PutSkippedKnown.Increment()
		b.noteSkip()
		return nil
	}
	b.addKeyLocked(h)
	b.keysMu.Unlock()
	// The body came from this machine, so this process has it whatever the upload goes on to do.
	b.noteHeld(h)
	b.track(h, pendingPut{actionID: actionID, outputID: outputID, path: path, data: data})

	j := putJob{actionID: actionID, key: b.key(actionID), hash: h, outputID: outputID, data: data, path: path}
	if !b.prep.submit(j) {
		// Nowhere to run the work: drop the claim so a later run re-uploads.
		b.removeClaimed(h)
	}
	return nil
}

// prepare turns a claimed object into a queued upload: the guards that decide
// whether it may be published at all, then lz4, then the metadata. It runs on
// a prep worker. Compression is the expensive half. That moment is one that
// goroutine could have started the next compile instead.
func (b *WebBackend) prepare(j putJob) {
	// After a hand-off the spool carries this upload. Work here is waste.
	if b.aborted() {
		return
	}
	// A file-backed job carries a path, and the body is read here rather than by
	// the caller.
	if j.data == nil && j.path != "" {
		data, err := os.ReadFile(j.path)
		if err != nil {
			logging.Warnf("cacheprog: web put %s: read body: %v; not uploading this object", ShortID(j.actionID), err)
			b.dropUpload(j.hash)
			return
		}
		j.data = data
	}

	// Cross-contamination guard: refuse to publish a package under a key that
	// disagrees with its own build id. The body<->outputID hash alone cannot
	// catch a swapped (actionID, object) pair, so this is the only defense
	// against poisoning the cache.
	if act, ok := BuildIDMatchesAction(j.actionID, j.data); !ok {
		b.PutRefusedBuildID.Increment()
		logging.Warnf("cacheprog: web put %s (%s): refusing upload, build-id action mismatch (want action=%s, got action=%s)",
			ShortID(j.actionID), describeObject(j.data), ExpectedBuildIDAction(j.actionID), act)
		b.dropUpload(j.hash)
		return
	}

	// Never publish a Go module index: the read side can't verify it, so it refuses every
	// upload; recomputing locally is free.
	if IsGoModuleIndex(j.data) {
		b.PutRefusedModIndex.Increment()
		b.dropUpload(j.hash)
		return
	}

	compressStart := time.Now()
	compressed, err := Compress(j.data)
	if b.Latency != nil {
		b.Latency.Compress.Record(time.Since(compressStart))
	}
	if err != nil {
		logging.Warnf("cacheprog: web put %s: compress: %v", ShortID(j.actionID), err)
		b.dropUpload(j.hash)
		return
	}
	b.CompressedBytes.Add(uint32(len(compressed)))
	b.RawBytes.Add(uint32(len(j.data)))

	// meta holds lowercased names without the X-Cache-Meta- prefix;
	// metadataHeaders derives the single-PUT headers from this same map, keeping
	// both paths in sync.
	meta := map[string]string{
		"outputid":          j.outputID,
		StoredDigestMetaKey: StoredDigest(compressed),
		"object-type":       detectObjectType(j.data),
		"body-size":         strconv.Itoa(len(j.data)),
		"compression":       "zstd",
		"created":           time.Now().UTC().Format(time.RFC3339),
	}
	if b.version != "" {
		meta["toolchain-version"] = b.version
	}
	if module := b.moduleName(); module != "" {
		meta["module"] = module
	}
	if goVer, target := parseArchiveHeader(j.data); goVer != "" {
		meta["go-version"] = goVer
		meta["target"] = target
	}
	if pkg := parseImportPath(j.data); pkg != "" {
		meta["pkg"] = pkg
	}
	if files := parseSourceFiles(j.data); len(files) > 0 {
		meta["src"] = capSrcList(files)
	}

	pr := putReq{
		actionID:   j.actionID,
		key:        j.key,
		hash:       j.hash,
		outputID:   j.outputID,
		compressed: compressed,
		metadata:   meta,
	}

	// No batch endpoint (learned from an earlier refusal): fall back to the single-PUT path.
	if b.batchPutUnsupported.Load() {
		if err := b.putSingle(pr); err != nil && !isLoggedErr(err) {
			logging.Warnf("cacheprog: web put %s: %v", ShortID(pr.actionID), err)
		}
		return
	}

	// Enqueue onto the coalescer, which now owns the claim, until a per-object or whole-batch failure rolls it back.
	select {
	case b.putBatchReqCh <- pr:
	case <-b.putBatchStop:
		// Backend is closing — drop the claim so a later run re-uploads.
		b.removeClaimed(pr.hash)
	}
}

// metadataHeaders renders the meta map back into X-Cache-Meta-* headers, the inverse of
// the map built in Put.
func metadataHeaders(meta map[string]string) http.Header {
	h := http.Header{}
	for name, val := range meta {
		h.Set("X-Cache-Meta-"+name, val)
	}
	return h
}

// srcMetaMaxFiles/srcMetaMaxBytes bound the Src metadata value so it always fits the
// cache server's shared ext4 xattr block.
const (
	srcMetaMaxFiles = 8
	srcMetaMaxBytes = 256
)

// capSrcList renders a source-file basename list as the Src metadata value,
// bounded to at most srcMetaMaxFiles names and srcMetaMaxBytes bytes in total.
// Names past the cap are summarized as a trailing "+N more".
func capSrcList(files []string) string {
	total := len(files)
	if len(files) > srcMetaMaxFiles {
		files = files[:srcMetaMaxFiles]
	}
	for {
		s := strings.Join(files, " ")
		if dropped := total - len(files); dropped > 0 {
			suffix := "+" + strconv.Itoa(dropped) + " more"
			if s == "" {
				s = suffix
			} else {
				s += " " + suffix
			}
		}
		if len(s) <= srcMetaMaxBytes || len(files) == 0 {
			return s
		}
		files = files[:len(files)-1]
	}
}
