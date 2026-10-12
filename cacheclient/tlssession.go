package cacheclient

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
)

// diskSessions is a TLS session cache that a process shares with the
// processes after it.
type diskSessions struct {
	dir string
	mem tls.ClientSessionCache
}

func newDiskSessions(dir string) *diskSessions {
	if dir == "" {
		dir = os.TempDir()
	}
	return &diskSessions{
		dir: filepath.Join(dir, "tls-sessions"),
		mem: tls.NewLRUClientSessionCache(0),
	}
}

func (cache *diskSessions) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(cache.dir, hex.EncodeToString(sum[:]))
}

func (cache *diskSessions) Get(key string) (*tls.ClientSessionState, bool) {
	if state, ok := cache.mem.Get(key); ok {
		return state, true
	}
	raw, err := os.ReadFile(cache.path(key))
	if err != nil || len(raw) < 4 {
		return nil, false
	}
	size := binary.BigEndian.Uint32(raw)
	if uint64(len(raw)-4) < uint64(size) {
		return nil, false
	}
	ticket, rest := raw[4:4+size], raw[4+size:]
	parsed, err := tls.ParseSessionState(rest)
	if err != nil {
		return nil, false
	}
	state, err := tls.NewResumptionState(ticket, parsed)
	if err != nil {
		return nil, false
	}
	cache.mem.Put(key, state)
	return state, true
}

// Put stores the session in memory and on disk. A session that fails to
// store costs the next process a full handshake and nothing else.
func (cache *diskSessions) Put(key string, state *tls.ClientSessionState) {
	cache.mem.Put(key, state)
	path := cache.path(key)
	if state == nil {
		os.Remove(path)
		return
	}
	ticket, parsed, err := state.ResumptionState()
	if err != nil || parsed == nil {
		return
	}
	body, err := parsed.Bytes()
	if err != nil {
		return
	}
	raw := binary.BigEndian.AppendUint32(nil, uint32(len(ticket)))
	raw = append(raw, ticket...)
	raw = append(raw, body...)
	if err := os.MkdirAll(cache.dir, 0o700); err != nil {
		return
	}
	tmp, err := os.CreateTemp(cache.dir, ".tmp-*")
	if err != nil {
		return
	}
	_, werr := tmp.Write(raw)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Rename(tmp.Name(), path) != nil {
		os.Remove(tmp.Name())
	}
}
