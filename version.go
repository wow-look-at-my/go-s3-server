package main

import (
	"encoding/json"
	"net/http"
	"runtime"
	"runtime/debug"
)

// versionPath answers what build is serving. It sits with /_health, before
// the auth gate, so an operator checks a deploy with one unauthenticated GET.
const versionPath = "/_version"

// buildVersion is what /_version reports. Every field comes from the binary's
// own build information, so a deploy cannot claim a version it is not.
type buildVersion struct {
	Version  string `json:"version"`  // the main module's version, or (devel)
	Revision string `json:"revision"` // the VCS commit the build came from
	Time     string `json:"time"`     // that commit's time, RFC 3339
	Modified bool   `json:"modified"` // the tree had uncommitted changes
	Go       string `json:"go"`       // the Go toolchain that built it
}

// currentBuild reads the build information the linker stamped. A binary with
// none, such as a test binary, reports the Go version alone.
func currentBuild() buildVersion {
	v := buildVersion{Go: runtime.Version()}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return v
	}
	v.Version = info.Main.Version
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			v.Revision = s.Value
		case "vcs.time":
			v.Time = s.Value
		case "vcs.modified":
			v.Modified = s.Value == "true"
		}
	}
	return v
}

func handleVersion(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(currentBuild())
}
