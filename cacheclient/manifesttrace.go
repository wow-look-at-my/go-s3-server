// ManifestTraceEnv names a file that takes one line per manifest event: "<event> <action ID>".

package cacheclient

import (
	"os"
	"strings"
	"sync"
)

// ManifestTraceEnv names a file that takes one line per manifest event: "<event> <action ID>".
const ManifestTraceEnv = "GO_CACHE_MANIFEST_TRACE"

type manifestTrace struct {
	mu    sync.Mutex
	path  string
	lines []string
}

func newManifestTrace() *manifestTrace {
	path := os.Getenv(ManifestTraceEnv)
	if path == "" {
		return nil
	}
	return &manifestTrace{path: path}
}

func (tr *manifestTrace) add(event, actionID string) {
	if tr == nil {
		return
	}
	tr.mu.Lock()
	tr.lines = append(tr.lines, event+" "+actionID)
	tr.mu.Unlock()
}

// write appends the events to the trace file.
func (tr *manifestTrace) write() {
	if tr == nil {
		return
	}
	tr.mu.Lock()
	body := strings.Join(tr.lines, "\n")
	tr.lines = nil
	tr.mu.Unlock()
	if body == "" {
		return
	}
	file, err := os.OpenFile(tr.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		logging.Warnf("cacheprog: manifest trace: %v", err)
		return
	}
	defer file.Close()
	if _, err := file.WriteString(body + "\n"); err != nil {
		logging.Warnf("cacheprog: manifest trace: %v", err)
	}
}
