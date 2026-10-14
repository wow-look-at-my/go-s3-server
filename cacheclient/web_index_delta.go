package cacheclient

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// A disk copy of the index lacks every key stored since it was written.

// Header and query names of the add-log position. The server carries the same names.
const (
	HeaderIndexEpoch = "X-Index-Epoch"
	HeaderIndexSeq   = "X-Index-Seq"
	HeaderIndexDelta = "X-Index-Delta"
)

// indexGen is a position in the server's add log. A zero epoch is no position.
type indexGen struct {
	epoch uint64
	seq   uint64
}

func indexGenFromHeader(h http.Header) indexGen {
	epoch, err := strconv.ParseUint(h.Get(HeaderIndexEpoch), 10, 64)
	if err != nil {
		return indexGen{}
	}
	seq, err := strconv.ParseUint(h.Get(HeaderIndexSeq), 10, 64)
	if err != nil {
		return indexGen{}
	}
	return indexGen{epoch: epoch, seq: seq}
}

// indexGenPath holds the position of the copy at path, and the copy's ETag.
func indexGenPath(path string) string { return path + ".gen" }

func writeIndexGen(path, etag string, gen indexGen) {
	if gen.epoch == 0 || etag == "" {
		_ = os.Remove(indexGenPath(path))
		return
	}
	line := fmt.Sprintf("%d %d %s\n", gen.epoch, gen.seq, etag)
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".gen.tmp*")
	if err != nil {
		return
	}
	_, werr := tmp.WriteString(line)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Rename(tmp.Name(), indexGenPath(path)) != nil {
		_ = os.Remove(tmp.Name())
	}
}

// readIndexGen answers the copy's position, or none when the file is missing,
// malformed or written for a different copy.
func readIndexGen(path, etag string) indexGen {
	data, err := os.ReadFile(indexGenPath(path))
	if err != nil {
		return indexGen{}
	}
	fields := strings.Fields(string(data))
	if len(fields) != 3 || fields[2] != etag {
		return indexGen{}
	}
	epoch, err1 := strconv.ParseUint(fields[0], 10, 64)
	seq, err2 := strconv.ParseUint(fields[1], 10, 64)
	if err1 != nil || err2 != nil || epoch == 0 {
		return indexGen{}
	}
	return indexGen{epoch: epoch, seq: seq}
}

// LiveDeltaInterval is the least time between live delta refreshes. A burst of misses then costs a single request.
const LiveDeltaInterval = 2 * time.Second

// setLiveGen records the position the live set is complete up to.
func (b *WebBackend) setLiveGen(gen indexGen) {
	b.keysMu.Lock()
	b.liveGen = gen
	b.keysMu.Unlock()
}

// refreshLiveDelta adds the keys stored since the live set's position, at
// most once per LiveDeltaInterval. Concurrent callers wait for the request in
// flight rather than send their own. It reports whether a delta was applied.
// A set with no position, or a load still in progress, is left alone.
func (b *WebBackend) refreshLiveDelta() bool {
	b.deltaMu.Lock()
	defer b.deltaMu.Unlock()
	if time.Since(b.deltaAt) < LiveDeltaInterval {
		return false
	}
	b.keysMu.RLock()
	gen, loading, authoritative := b.liveGen, b.keysJournal != nil, b.indexAuthoritative
	b.keysMu.RUnlock()
	if gen.epoch == 0 || loading || !authoritative {
		return false
	}
	b.deltaAt = time.Now()
	reply, err := b.doIndexGET(context.Background(), deltaQuery(gen), "")
	if reply.status == http.StatusGone {
		// The epoch ended. The set stays as it is, and no later miss asks again.
		logging.Infof("cacheprog: web index: the server's add log moved past this process's position; misses stop refreshing")
		b.setLiveGen(indexGen{})
		return false
	}
	if err != nil || !reply.delta {
		if err != nil {
			logging.Infof("cacheprog: web index live delta: %v", err)
		}
		return false
	}
	added, _, err := parseIndexBlob(reply.body)
	if err != nil {
		logging.Warnf("cacheprog: web index live delta parse: %v", err)
		return false
	}
	b.keysMu.Lock()
	b.keys.addAll(added)
	b.liveGen = reply.gen
	b.keysMu.Unlock()
	return true
}

// persistDeltaMin is the least delta worth a rewrite of the copy.
var persistDeltaMin = 16384

// persistDelta writes the copy merged with added, at position gen, over the
// copy at path.
func (b *WebBackend) persistDelta(path string, disk diskIndex, added *hashSet, gen indexGen) {
	if added.Len() > 0 && added.Len() < persistDeltaMin {
		return
	}
	if added.Len() == 0 {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
		writeIndexGen(path, disk.etag, gen)
		return
	}
	blob := mergeIndexBlob(disk.blob, added)
	_, etag, err := parseIndexBlob(blob)
	if err != nil {
		logging.Warnf("cacheprog: web index: merged copy does not parse: %v", err)
		return
	}
	b.writeIndexBlob(path, blob)
	writeIndexGen(path, etag, gen)
}

// mergeIndexBlob is a short-form GBCI blob of the keys in base and added.
// base is a valid blob of either width.
func mergeIndexBlob(base []byte, added *hashSet) []byte {
	width := int(base[5])
	count := int(binary.LittleEndian.Uint64(base[16:24]))
	extra := slices.Sorted(added.All())
	merged := make([]hashKey, 0, count+len(extra))
	next := 0
	for idx := range count {
		off := gbciHeaderSize + idx*width
		key := binary.BigEndian.Uint64(base[off : off+gbciPrefixSize])
		for next < len(extra) && extra[next] < key {
			merged = append(merged, extra[next])
			next++
		}
		if next < len(extra) && extra[next] == key {
			next++
		}
		if len(merged) == 0 || merged[len(merged)-1] != key {
			merged = append(merged, key)
		}
	}
	merged = append(merged, extra[next:]...)
	return marshalSortedIndex(merged)
}

func deltaQuery(gen indexGen) string {
	q := url.Values{}
	q.Set("epoch", strconv.FormatUint(gen.epoch, 10))
	q.Set("seq", strconv.FormatUint(gen.seq, 10))
	return q.Encode()
}

// startDeltaLoad installs a young disk copy as non-authoritative and brings it
// current in the background.
func (b *WebBackend) startDeltaLoad(path string, disk diskIndex) {
	b.keysMu.Lock()
	b.keys = disk.keys
	b.indexAuthoritative = false
	b.indexEmpty = disk.count == 0
	b.indexKeysAtStart = disk.count
	b.keysJournal = &keysJournal{added: set.New[actionHash](), dropped: set.New[actionHash]()}
	b.keysMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	load := &indexLoad{cancel: cancel, done: make(chan struct{})}
	b.indexLoad.Store(load)
	go func() {
		defer close(load.done)
		defer cancel()
		b.applyDelta(ctx, path, disk)
	}()
	logging.Infof("cacheprog: web index: %d keys from a copy %v old; fetching the keys added since", disk.count, disk.age().Round(time.Second))
}

// applyDelta adds the keys stored since the copy to the live set and makes it
// authoritative. A server that cannot answer from the copy's position gets a
// full refresh. A failed request leaves the copy non-authoritative.
func (b *WebBackend) applyDelta(ctx context.Context, path string, disk diskIndex) {
	reply, err := b.doIndexGET(ctx, deltaQuery(disk.gen), "")
	if err != nil && reply.status != http.StatusGone {
		if ctx.Err() == nil {
			logging.Infof("cacheprog: web index delta: %v; using %d cached keys (batch probing enabled)", err, disk.count)
		}
		b.adoptIndex(nil, false)
		return
	}
	if reply.status == http.StatusOK && reply.delta {
		added, _, perr := parseIndexBlob(reply.body)
		if perr != nil {
			logging.Warnf("cacheprog: web index delta parse: %v; using %d cached keys (batch probing enabled)", perr, disk.count)
			b.adoptIndex(nil, false)
			return
		}
		// The live set already carries every claim and drop, so the journal has nothing to replay.
		b.keysMu.Lock()
		b.keys.addAll(added)
		b.keysJournal = nil
		b.liveGen = reply.gen
		b.indexAuthoritative = true
		b.indexEmpty = b.keys.Len() == 0
		b.keysMu.Unlock()
		logging.Infof("cacheprog: web index: %d keys added since the copy", added.Len())
		// The next go command then asks only for what this did not see.
		b.persistDelta(path, disk, added, reply.gen)
		return
	}
	keys, authoritative, _ := b.refreshIndex(ctx, path, disk)
	b.adoptIndex(keys, authoritative)
}
