//go:build !windows

package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The metadata write and the uncached metadata read, with the fields a real
// build upload carries. These miss the metadata cache on purpose.
func BenchmarkPutWithAudit(b *testing.B) {
	dir := b.TempDir()
	storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(b, err)
	b.Cleanup(func() { storage.Close() })

	body := benchBody(b, 8<<10)
	payload := lz4Compress(b, body)
	meta := benchMeta(body)
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		audit := map[string]string{"uploader": "bench", "client_ip": "10.0.0.1", "user_agent": "go-toolchain"}
		key := fmt.Sprintf("go-buildcache/v1%064x", idx%64)
		require.NoError(b, storage.PutStream(key, bytes.NewReader(payload), meta, audit))
	}
}

func BenchmarkReadMetadataUncached(b *testing.B) {
	dir := b.TempDir()
	storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(b, err)
	b.Cleanup(func() { storage.Close() })

	key := "go-buildcache/v1" + strings.Repeat("8", 64)
	body := benchBody(b, 8<<10)
	audit := map[string]string{"uploader": "bench", "client_ip": "10.0.0.1"}
	require.NoError(b, storage.PutStream(key, bytes.NewReader(lz4Compress(b, body)), benchMeta(body), audit))
	path := storage.keyToPath(key)

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		meta := &ObjectMeta{Metadata: map[string]string{}}
		getMetadata(path, meta)
		require.NotEmpty(b, meta.Metadata["outputid"])
	}
}
