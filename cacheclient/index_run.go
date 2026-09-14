package cacheclient

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// indexRunIdle is the gap in index use past which the recorded run is taken
// to be over and the next use starts a new one. A build's go commands follow
// each other far closer than this.
const indexRunIdle = 5 * time.Minute

// indexRunMarkerPath is where the run marker for an index copy lives. It sits
// beside the copy, so every process sharing an IndexDir shares one run.
func indexRunMarkerPath(indexPath string) string {
	return indexPath + ".run"
}

// indexRunWindow returns how long the run this process belongs to has been
// using the index at indexPath, and records the use.
//
// The run is the marker beside the copy: its content is when the run began,
// its mtime is when the index was last used. A marker untouched for longer
// than indexRunIdle belongs to a finished run, so this use begins a new one
// and the window is zero.
func indexRunWindow(indexPath string, now time.Time) time.Duration {
	path := indexRunMarkerPath(indexPath)
	start := now
	if st, err := os.Stat(path); err == nil && now.Sub(st.ModTime()) < indexRunIdle {
		if s, ok := readIndexRunStart(path); ok && !s.After(now) {
			start = s
		}
	}
	writeIndexRunStart(path, start)
	return now.Sub(start)
}

// readIndexRunStart reads the run start out of the marker at path.
func readIndexRunStart(path string) (time.Time, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, false
	}
	nanos, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, nanos), true
}

// writeIndexRunStart records start in the marker at path and stamps the
// marker's mtime with this use. It is best effort: an index directory this
// process cannot write leaves the default at its floor, which is what the
// floor is for.
func writeIndexRunStart(path string, start time.Time) {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return
	}
	name := tmp.Name()
	_, werr := tmp.WriteString(strconv.FormatInt(start.UnixNano(), 10))
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		os.Remove(name)
		return
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
	}
}
