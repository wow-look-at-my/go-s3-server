package cacheclient

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
)

// BuildIDHashSize is how many leading action-hash bytes cmd/go stamps into a build id's ACTION field, base64.RawURLEncoding'd.
const BuildIDHashSize = 15

// ExpectedBuildIDAction derives the build-id ACTION field cmd/go would stamp
// for cache action actionIDHex (base64.RawURLEncoding of its leading bytes).
// Returns "" when actionIDHex is too short to derive -- not a mismatch.
func ExpectedBuildIDAction(actionIDHex string) string {
	raw, err := hex.DecodeString(actionIDHex)
	if err != nil || len(raw) < BuildIDHashSize {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw[:BuildIDHashSize])
}

// ArchiveExportInfo inspects data as a Go compiled-package object and reports
// isPkgArchive (an ar archive with a __.PKGDEF export-data member) and action
// (the field before '/' in `build id "ACTION/CONTENT"`, or "" if none). A
// non-archive yields (false, ""). Only the header before the "$$" export-data
// marker is scanned, so binary export bytes can't be mistaken for a build id.
func ArchiveExportInfo(data []byte) (isPkgArchive bool, action string) {
	pkgdef := arMember(data, "__.PKGDEF")
	if pkgdef == nil {
		return false, ""
	}
	header := pkgdef
	if i := bytes.Index(header, []byte("$$")); i >= 0 {
		header = header[:i]
	}
	const marker = `build id "`
	idx := bytes.Index(header, []byte(marker))
	if idx < 0 {
		return true, ""
	}
	rest := header[idx+len(marker):]
	end := bytes.IndexByte(rest, '"')
	if end < 0 {
		return true, ""
	}
	id := rest[:end]
	if slash := bytes.IndexByte(id, '/'); slash >= 0 {
		id = id[:slash]
	}
	return true, string(id)
}

// archiveBuildIDAction returns the ACTION field of data's build id (see
// ArchiveExportInfo), or "" if data has no build id line.
func archiveBuildIDAction(data []byte) string {
	_, action := ArchiveExportInfo(data)
	return action
}

// BuildIDMatchesAction reports whether body is consistent with cache action
// actionIDHex, catching a self-consistent body stored under the wrong key.
// got is the archive's stamped action. ok is false only when the build id
// proves a DIFFERENT action, or a package archive has none (cmd/go always
// stamps it). Best-effort integrity, not an authorization boundary.
func BuildIDMatchesAction(actionIDHex string, body []byte) (got string, ok bool) {
	isPkg, action := ArchiveExportInfo(body)
	want := ExpectedBuildIDAction(actionIDHex)

	if action != "" {
		if want == "" {
			return action, true // no derivable expectation; don't false-positive
		}
		return action, action == want
	}

	// A package archive with no build id, checked against a real key, is refused.
	// A non-archive entry falls through, guarded only by the hash.
	if isPkg && want != "" {
		return "", false
	}
	return "", true
}
