package cacheclient

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"
)

// A build does not wait at exit for its uploads.

// UploadSpoolEnv names the spool file that a detached uploader process runs.
const UploadSpoolEnv = "GO_CACHE_UPLOAD_SPOOL"

// spoolDirName is the spool directory under the local cache directory.
const spoolDirName = "upload-spool"

// spoolStaleAfter is the age after which the sweep restarts a spool whose lock is free.
var spoolStaleAfter = 30 * time.Second

// startUploader starts the uploader for a spool. A test replaces it.
var startUploader = spawnUploader

// handOffOn is set by a consumer whose executable serves UploadSpoolEnv.
var handOffOn atomic.Bool

// HandOffUploads tells Close to hand unconfirmed uploads to a detached uploader.
func HandOffUploads() { handOffOn.Store(true) }

// pendingPut is an upload this backend claimed and the store has not confirmed.
type pendingPut struct {
	actionID string
	outputID string
	path     string
	data     []byte
}

// spoolFile is what a spool holds: the store settings that shape a key and
// its metadata, and every upload still owed.
type spoolFile struct {
	Prefix   string       `json:"prefix"`
	Version  string       `json:"version,omitempty"`
	Module   string       `json:"module,omitempty"`
	Target   string       `json:"target,omitempty"`
	Command  string       `json:"command,omitempty"`
	IndexDir string       `json:"index_dir,omitempty"`
	Entries  []spoolEntry `json:"entries"`
	// FetchIndex asks the uploader to finish the index download that the build cancelled at exit.
	FetchIndex bool `json:"fetch_index,omitempty"`
}

// spoolEntry is one upload. Path names a body in the local cache; Data
// carries a body that has no file. Replace drops a stale claim first, as the
// manifest store does.
type spoolEntry struct {
	ActionID string `json:"action_id"`
	OutputID string `json:"output_id"`
	Path     string `json:"path,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Replace  bool   `json:"replace,omitempty"`
}

// track records a claimed upload until settle or dropUpload clears it.
func (b *WebBackend) track(h actionHash, p pendingPut) {
	b.pendingMu.Lock()
	if b.pending == nil {
		b.pending = map[actionHash]pendingPut{}
	}
	b.pending[h] = p
	b.pendingMu.Unlock()
}

// settle clears an upload the store confirmed.
func (b *WebBackend) settle(h actionHash) {
	b.pendingMu.Lock()
	delete(b.pending, h)
	b.pendingMu.Unlock()
}

// dropUpload gives up an upload on purpose: a guard refused it, or its body is gone.
func (b *WebBackend) dropUpload(h actionHash) {
	b.removeClaimed(h)
	b.settle(h)
}

// pendingCount reports how many uploads the store has not confirmed.
func (b *WebBackend) pendingCount() int {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	return len(b.pending)
}

// HandedOff reports how many uploads Close gave to the detached uploader.
func (b *WebBackend) HandedOff() int64 { return b.handedOff.Load() }

// requestCtx is the context of every upload and look-ahead request.
func (b *WebBackend) requestCtx() context.Context {
	if b.abortCtx == nil {
		return context.Background()
	}
	return b.abortCtx
}

// abortDone is closed when Close hands the uploads off.
func (b *WebBackend) abortDone() <-chan struct{} {
	if b.abortCtx == nil {
		return nil
	}
	return b.abortCtx.Done()
}

// aborted reports whether Close has handed the uploads off.
func (b *WebBackend) aborted() bool {
	return b.abortCtx != nil && b.abortCtx.Err() != nil
}

// handsOff reports whether Close spools the uploads rather than drains them.
func (b *WebBackend) handsOff() bool {
	return canSpawn && handOffOn.Load() && b.spoolDir != "" && !b.drainOnClose
}

// closeHandOff is Close for a backend that hands its uploads off. It cancels
// every request in flight, writes what the store has not confirmed to a
// spool, starts the uploader on it, and returns. A cancelled upload is not
// lost: it stays pending, so the spool carries it.
func (b *WebBackend) closeHandOff() error {
	b.abort()
	fetchIndex := false
	if l := b.indexLoad.Load(); l != nil {
		select {
		case <-l.done:
		default:
			// A short command ends before the download does. With no copy on disk, the next command would start it again.
			_, err := os.Stat(b.indexCachePath())
			fetchIndex = os.IsNotExist(err)
		}
		l.cancel()
		<-l.done
	}
	b.stopManifest()
	b.manifestWG.Wait()
	manifests := b.manifestToStore()
	b.reportManifest()
	b.prep.Close()
	if b.putBatchStop != nil {
		close(b.putBatchStop)
		<-b.putBatchDone
	}
	if b.batchStop != nil {
		close(b.batchStop)
		<-b.batchDone
	}
	b.skips.wg.Wait()
	b.prefetchWG.Wait()
	b.lookAhead.Close()

	entries := b.pendingEntries()
	for _, write := range manifests {
		entries = append(entries, spoolEntry{ActionID: write.id, OutputID: StoredDigest(write.body), Data: write.body, Replace: true})
	}
	if len(entries) > 0 || fetchIndex {
		if err := b.handOff(entries, fetchIndex); err != nil {
			logging.Warnf("cacheprog: upload hand-off: %v; %d uploads are not stored", err, len(entries))
		} else {
			b.handedOff.Store(int64(len(entries)))
		}
	}
	if b.errLog != nil {
		_ = b.errLog.Close()
	}
	return nil
}

// pendingEntries answers every unconfirmed upload as a spool entry.
func (b *WebBackend) pendingEntries() []spoolEntry {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	entries := make([]spoolEntry, 0, len(b.pending))
	for _, p := range b.pending {
		entries = append(entries, spoolEntry{ActionID: p.actionID, OutputID: p.outputID, Path: p.path, Data: p.data})
	}
	return entries
}

// handOff writes entries to a new spool and starts its uploader.
func (b *WebBackend) handOff(entries []spoolEntry, fetchIndex bool) error {
	spool := spoolFile{
		Prefix:     b.prefix,
		Version:    b.version,
		Module:     b.moduleName(),
		Target:     b.target,
		Command:    b.command,
		IndexDir:   b.indexDir,
		Entries:    entries,
		FetchIndex: fetchIndex,
	}
	path, err := writeSpool(b.spoolDir, spool)
	if err != nil {
		return err
	}
	if err := startUploader(path); err != nil {
		// The spool stays. The sweep of a later cache owner starts it.
		return fmt.Errorf("start the uploader for %s: %w", path, err)
	}
	return nil
}

// writeSpool writes spool into dir under a name no other process uses. The
// rename makes it whole before any reader can see it.
func writeSpool(dir string, spool spoolFile) (string, error) {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return "", err
	}
	data, err := json.Marshal(spool)
	if err != nil {
		return "", err
	}
	var tag [8]byte
	if _, err := rand.Read(tag[:]); err != nil {
		return "", err
	}
	path := filepath.Join(dir, strconv.Itoa(os.Getpid())+"-"+hex.EncodeToString(tag[:])+".json")
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return "", err
	}
	_, werr := tmp.Write(data)
	cerr := tmp.Close()
	if err := errors.Join(werr, cerr); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return path, nil
}

// RunUploadSpool stores every upload the spool at path holds, then deletes
// the spool. It drains before it returns. A spool that another uploader has
// locked is left to that uploader, and RunUploadSpool returns nil.
func RunUploadSpool(path string) error {
	unlock, err := cachedisk.TryLockFile(spoolLockPath(path))
	if err != nil {
		return nil
	}
	locked := true
	release := func() {
		if locked {
			unlock()
			locked = false
		}
	}
	defer release()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// Another uploader finished this spool and deleted it.
		release()
		os.Remove(spoolLockPath(path))
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) == 0 {
		// An older client locked the spool itself, which created an empty one once its uploader deleted it.
		release()
		removeSpool(path)
		return nil
	}
	var spool spoolFile
	if err := json.Unmarshal(data, &spool); err != nil {
		release()
		removeSpool(path)
		return fmt.Errorf("upload spool %s: %w; it is deleted", path, err)
	}
	cfg := ConfigFromEnv()
	if cfg.Bucket == "" {
		return fmt.Errorf("upload spool %s: %s names no store, so the spool stays", path, ConfigEnv)
	}
	cfg.Prefix = spool.Prefix
	cfg.Version = spool.Version
	cfg.Module = spool.Module
	cfg.Target = spool.Target
	cfg.Command = spool.Command
	cfg.IndexDir = spool.IndexDir
	store, err := NewWebBackend(cfg)
	if err != nil {
		return fmt.Errorf("upload spool %s: %w", path, err)
	}
	store.drainOnClose = true
	store.skipIndex()

	gone := 0
	for _, entry := range spool.Entries {
		if entry.Replace {
			store.ForgetStale(entry.ActionID)
		}
		if entry.Path == "" {
			err = store.Put(entry.ActionID, entry.OutputID, entry.Data)
		} else if _, statErr := os.Stat(entry.Path); statErr != nil {
			gone++
			continue
		} else {
			err = store.PutFile(entry.ActionID, entry.OutputID, entry.Path)
		}
		if err != nil {
			logging.Warnf("cacheprog: upload spool: %v", err)
		}
	}
	store.Close()
	if spool.FetchIndex {
		fetchIndexCopy(cfg)
	}
	failed := store.pendingCount()
	logging.Infof("cacheprog: upload spool %s: %d of %d stored, %d bodies gone, %d failed",
		filepath.Base(path), store.Stats.Puts.Load(), len(spool.Entries), gone, failed)
	// The lock goes first: a file that is open cannot be removed on Windows.
	release()
	if err := removeSpool(path); err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("upload spool %s: %d uploads failed", path, failed)
	}
	return nil
}

// spoolLockPath is the file an uploader locks while it runs the spool at
// path.
func spoolLockPath(path string) string {
	return path + ".lock"
}

// removeSpool deletes the spool at path and its lock file.
func removeSpool(path string) error {
	err := os.Remove(path)
	os.Remove(spoolLockPath(path))
	return err
}

// fetchIndexCopy downloads the key index into its disk copy for the next
// command, and waits for the download to end.
func fetchIndexCopy(cfg WebConfig) {
	idx, err := NewWebBackend(cfg)
	if err != nil {
		logging.Warnf("cacheprog: upload spool: index download: %v", err)
		return
	}
	idx.drainOnClose = true
	idx.ensureIndex()
	if l := idx.indexLoad.Load(); l != nil {
		<-l.done
	}
	idx.Close()
}

// skipIndex makes this backend upload with no key index: every upload a
// spool holds is one the store did not confirm, whatever an index says.
func (b *WebBackend) skipIndex() {
	b.indexOnce.Do(func() {
		b.keysMu.Lock()
		b.keys = newHashSet(0)
		b.keysMu.Unlock()
	})
}

// sweepSpools starts an uploader for every spool in dir that lost its own. A
// spool younger than spoolStaleAfter can still be waiting for its uploader to
// lock it, so it is left alone.
func sweepSpools(dir string) {
	list, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	now := time.Now()
	for _, item := range list {
		name := item.Name()
		info, err := item.Info()
		if err != nil || now.Sub(info.ModTime()) < spoolStaleAfter {
			continue
		}
		path := filepath.Join(dir, name)
		switch {
		case strings.HasPrefix(name, ".tmp-"):
			os.Remove(path)
		case strings.HasSuffix(name, ".json.log"):
			// A log goes once its spool is gone.
			if _, err := os.Stat(strings.TrimSuffix(path, ".log")); err != nil {
				os.Remove(path)
			}
		case strings.HasSuffix(name, ".json.lock"):
			// A lock goes once its spool is gone.
			if _, err := os.Stat(strings.TrimSuffix(path, ".lock")); err != nil {
				os.Remove(path)
			}
		case strings.HasSuffix(name, ".json"):
			if info.Size() == 0 {
				os.Remove(path)
				continue
			}
			unlock, err := cachedisk.TryLockFile(spoolLockPath(path))
			if err != nil {
				continue
			}
			unlock()
			if err := startUploader(path); err != nil {
				logging.Warnf("cacheprog: upload spool %s: start the uploader: %v", name, err)
			}
		}
	}
}
