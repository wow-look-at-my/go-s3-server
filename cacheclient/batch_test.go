package cacheclient

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildBatchTar creates a tar stream matching the server's /_batch/get format.
func buildBatchTar(t *testing.T, manifest batchGetManifest, data map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	mdata, _ := json.Marshal(manifest)
	tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
	tw.Write(mdata)

	for _, e := range manifest.Entries {
		d := data[e.Key]
		tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(d)), Mode: 0644})
		tw.Write(d)
	}
	tw.Close()
	return buf.Bytes()
}

func TestParseBatchResponse_Basic(t *testing.T) {
	manifest := batchGetManifest{
		Entries: []batchGetManifestEntry{
			{Key: "cache/v1aaa", Size: 6, Metadata: map[string]string{"outputid": "out-a"}},
			{Key: "cache/v1bbb", Size: 6, Metadata: map[string]string{"outputid": "out-b"}, Prefetch: true},
		},
	}
	data := map[string][]byte{
		"cache/v1aaa": []byte("data-a"),
		"cache/v1bbb": []byte("data-b"),
	}
	tarData := buildBatchTar(t, manifest, data)

	entries, err := parseBatchResponse(bytes.NewReader(tarData))
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.Equal(t, "cache/v1aaa", entries[0].Key)
	require.Equal(t, "out-a", entries[0].OutputID)
	require.Equal(t, "data-a", string(entries[0].Data))
	require.False(t, entries[0].Prefetch)

	require.Equal(t, "cache/v1bbb", entries[1].Key)
	require.Equal(t, "out-b", entries[1].OutputID)
	require.Equal(t, "data-b", string(entries[1].Data))
	require.True(t, entries[1].Prefetch)
}

func TestParseBatchResponse_Empty(t *testing.T) {
	manifest := batchGetManifest{Entries: []batchGetManifestEntry{}}
	tarData := buildBatchTar(t, manifest, nil)

	entries, err := parseBatchResponse(bytes.NewReader(tarData))
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestParseBatchResponse_MissingData(t *testing.T) {
	// Manifest references a key but the data/ tar entry is absent.
	// Build tar manually with only the manifest, no data entries.
	manifest := batchGetManifest{
		Entries: []batchGetManifestEntry{
			{Key: "cache/v1aaa", Size: 6, Metadata: map[string]string{"outputid": "out-a"}},
		},
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	mdata, _ := json.Marshal(manifest)
	tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
	tw.Write(mdata)
	tw.Close()

	entries, err := parseBatchResponse(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Empty(t, entries, "entries with no data should be skipped")
}
