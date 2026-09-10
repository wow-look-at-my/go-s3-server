package cacheclient

import (
	"github.com/wow-look-at-my/go-containers/set"

	"bytes"
	"encoding/binary"
	"path/filepath"
)

// parseImportPath extracts the import path from a Go ar archive's __.PKGDEF export data.
// Returns "" if data is not a recognised Go archive.
func parseImportPath(data []byte) string {
	p := openPkgbits(data)
	if p == nil {
		return ""
	}
	return p.readSectionString(pbSectionPkg, 0, pbSyncPkgDef)
}

// parseSourceFiles extracts the basenames of the Go source files compiled into
// a Go ar archive. Returns nil for non-archives or archives without pkgbits.
func parseSourceFiles(data []byte) []string {
	p := openPkgbits(data)
	if p == nil {
		return nil
	}
	n := p.sectionLen(pbSectionPosBase)
	seen := set.New[string](n)
	var files []string
	for i := 0; i < n; i++ {
		full := p.readSectionString(pbSectionPosBase, i, pbSyncPosBase)
		if full == "" {
			continue
		}
		base := filepath.Base(full)
		if filepath.Ext(base) == ".go" && seen.Add(base) {
			files = append(files, base)
		}
	}
	return files
}

// arMember finds and returns the body of a named member in an ar archive.
func arMember(data []byte, name string) []byte {
	const globalHdr = "!<arch>\n"
	if len(data) < len(globalHdr) || string(data[:len(globalHdr)]) != globalHdr {
		return nil
	}
	data = data[len(globalHdr):]

	// Member header: fixed-width name, mtime, uid, gid, mode, size and end marker.
	const hdrSize = 60
	for len(data) >= hdrSize {
		rawName := bytes.TrimRight(data[:16], " ")
		rawSize := bytes.TrimSpace(data[48:58])
		if string(data[58:60]) != "`\n" {
			return nil
		}
		size := 0
		for _, b := range rawSize {
			if b < '0' || b > '9' {
				return nil
			}
			size = size*10 + int(b-'0')
		}
		body := data[hdrSize:]
		if size > len(body) {
			return nil
		}
		// Strip BSD-style trailing slash from member name.
		memberName := string(bytes.TrimRight(rawName, "/"))
		if memberName == name {
			return body[:size]
		}
		// Members are padded to even byte boundaries.
		advance := hdrSize + size
		if size%2 != 0 {
			advance++
		}
		if advance > len(data) {
			return nil
		}
		data = data[advance:]
	}
	return nil
}

// pkgbits constants mirroring internal/pkgbits.
const (
	pbSectionString  = 0
	pbSectionPosBase = 2
	pbSectionPkg     = 3
	pbNumSections    = 10

	pbFlagSyncMarkers = 1

	// Sync marker values (the marker rides the varint's high bits, above its low byte).
	pbSyncRelocs   = 8
	pbSyncReloc    = 9
	pbSyncUseReloc = 10
	pbSyncUint64   = 4
	pbSyncString   = 5
	pbSyncPosBase  = 13
	pbSyncPkgDef   = 17
)

// pkgbitsPayload holds the decoded outer header of a pkgbits payload,
// providing section-element access without reparsing on each call.
type pkgbitsPayload struct {
	elemEndsEnds [pbNumSections]uint32
	elemEnds     []uint32
	elemData     []byte
	sync         bool
}

// openPkgbits locates and parses the pkgbits header from a Go ar archive.
// Returns nil if the data is not a recognised unified-IR archive.
func openPkgbits(data []byte) *pkgbitsPayload {
	pkgdef := arMember(data, "__.PKGDEF")
	if pkgdef == nil {
		return nil
	}
	// Layout: ...$$B\n | u<pkgbits-payload> | \n$$\n
	marker := []byte("$$B\n")
	idx := bytes.Index(pkgdef, marker)
	if idx < 0 {
		return nil
	}
	after := pkgdef[idx+len(marker):]
	if len(after) == 0 || after[0] != 'u' {
		return nil
	}
	payload := after[1:]
	if end := bytes.Index(payload, []byte("\n$$\n")); end >= 0 {
		payload = payload[:end]
	}
	return parsePkgbits(payload)
}

// parsePkgbits decodes the pkgbits outer header from a raw payload slice.
func parsePkgbits(payload []byte) *pkgbitsPayload {
	if len(payload) < 4 {
		return nil
	}
	version := binary.LittleEndian.Uint32(payload[:4])
	payload = payload[4:]

	var flags uint32
	if version >= 1 {
		if len(payload) < 4 {
			return nil
		}
		flags = binary.LittleEndian.Uint32(payload[:4])
		payload = payload[4:]
	}

	const endsEndsBytes = pbNumSections * 4
	if len(payload) < endsEndsBytes {
		return nil
	}
	var p pkgbitsPayload
	p.sync = flags&pbFlagSyncMarkers != 0
	for i := range p.elemEndsEnds {
		p.elemEndsEnds[i] = binary.LittleEndian.Uint32(payload[i*4:])
	}
	payload = payload[endsEndsBytes:]

	numElems := int(p.elemEndsEnds[pbNumSections-1])
	if len(payload) < numElems*4 {
		return nil
	}
	p.elemEnds = make([]uint32, numElems)
	for i := range p.elemEnds {
		p.elemEnds[i] = binary.LittleEndian.Uint32(payload[i*4:])
	}
	payload = payload[numElems*4:]

	const fingerprintSize = 8
	if len(payload) < fingerprintSize {
		return nil
	}
	p.elemData = payload[:len(payload)-fingerprintSize]
	return &p
}

// sectionLen returns the number of elements in section s.
func (p *pkgbitsPayload) sectionLen(s int) int {
	end := int(p.elemEndsEnds[s])
	start := 0
	if s > 0 {
		start = int(p.elemEndsEnds[s-1])
	}
	return end - start
}

// readSectionString returns the leading String() value from element relIdx within
// section s, which must be opened with the given openingSync marker.
// Returns "" on any parse error.
func (p *pkgbitsPayload) readSectionString(s, relIdx int, openingSync uint64) string {
	// Absolute element index.
	base := 0
	if s > 0 {
		base = int(p.elemEndsEnds[s-1])
	}
	absIdx := base + relIdx
	if absIdx >= int(p.elemEndsEnds[s]) {
		return ""
	}

	var start uint32
	if absIdx > 0 {
		start = p.elemEnds[absIdx-1]
	}
	end := p.elemEnds[absIdx]
	if int(end) > len(p.elemData) || start > end {
		return ""
	}
	r := bytes.NewReader(p.elemData[start:end])

	// Element header: reloc table -- [SyncRelocs][SyncUint64] nrelocs, then per-reloc fields.
	if !skipSync(r, p.sync, pbSyncRelocs) {
		return ""
	}
	if !skipSync(r, p.sync, pbSyncUint64) {
		return ""
	}
	nrelocs, err := rawUvarint(r)
	if err != nil {
		return ""
	}
	type reloc struct{ kind, idx uint64 }
	relocs := make([]reloc, nrelocs)
	for i := range relocs {
		if !skipSync(r, p.sync, pbSyncReloc) {
			return ""
		}
		if !skipSync(r, p.sync, pbSyncUint64) {
			return ""
		}
		kind, err := rawUvarint(r)
		if err != nil {
			return ""
		}
		if !skipSync(r, p.sync, pbSyncUint64) {
			return ""
		}
		idx, err := rawUvarint(r)
		if err != nil {
			return ""
		}
		relocs[i] = reloc{kind, idx}
	}

	// Opening sync marker.
	if !skipSync(r, p.sync, openingSync) {
		return ""
	}

	// String(): [sync(SyncString)] [sync(SyncUseReloc)] [sync(SyncUint64)] relocIdx
	if !skipSync(r, p.sync, pbSyncString) {
		return ""
	}
	if !skipSync(r, p.sync, pbSyncUseReloc) {
		return ""
	}
	if !skipSync(r, p.sync, pbSyncUint64) {
		return ""
	}
	relocIdx, err := rawUvarint(r)
	if err != nil {
		return ""
	}
	if int(relocIdx) >= len(relocs) {
		return ""
	}
	ref := relocs[relocIdx]
	if ref.kind != pbSectionString {
		return ""
	}

	// Resolve SectionString[ref.idx] — raw text bytes, no header.
	strBase := 0
	strEnd := int(p.elemEndsEnds[pbSectionString])
	strElemAbs := strBase + int(ref.idx)
	if strElemAbs >= strEnd {
		return ""
	}
	var strStart uint32
	if strElemAbs > 0 {
		strStart = p.elemEnds[strElemAbs-1]
	}
	strEndOff := p.elemEnds[strElemAbs]
	if int(strEndOff) > len(p.elemData) || strStart > strEndOff {
		return ""
	}
	return string(p.elemData[strStart:strEndOff])
}

// skipSync optionally reads and verifies a pkgbits sync marker varint.
// When sync is false it is a no-op. Returns false on read error or mismatch.
func skipSync(r *bytes.Reader, sync bool, marker uint64) bool {
	if !sync {
		return true
	}
	v, err := rawUvarint(r)
	if err != nil {
		return false
	}
	return v>>8 == marker
}

// rawUvarint reads an unsigned varint (little-endian, continuation-bit encoded).
func rawUvarint(r *bytes.Reader) (uint64, error) {
	var x uint64
	var s uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

// pkgbitsImportPath is kept for tests that call it directly.
func pkgbitsImportPath(payload []byte) string {
	p := parsePkgbits(payload)
	if p == nil {
		return ""
	}
	return p.readSectionString(pbSectionPkg, 0, pbSyncPkgDef)
}
