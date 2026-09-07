package cacheclient

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingReader reports how much of the stream has been consumed, so a test
// can ask what the reader had read at the moment an entry was handed over.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// bigBatch builds a response of n bodies of size bytes each.
func bigBatch(t *testing.T, n, size int) []byte {
	t.Helper()
	var m batchGetManifest
	data := map[string][]byte{}
	for i := range n {
		key := fmt.Sprintf("go-buildcache/v1%064x", i)
		body := bytes.Repeat([]byte{byte(i)}, size)
		data[key] = body
		m.Entries = append(m.Entries, batchGetManifestEntry{
			Key:      key,
			Size:     int64(len(body)),
			Metadata: map[string]string{"outputid": fmt.Sprintf("%064x", i), "body-size": fmt.Sprint(size)},
		})
	}
	return buildBatchTar(t, m, data)
}

// The point of streaming: a body reaches the consumer long before the response
// ends. A reader that buffered the whole tar would have consumed all of it
// before handing over anything, which is what made a batch response the
// client's largest resident cost.
func TestStreamBatchResponseDeliversBeforeTheStreamEnds(t *testing.T) {
	const n, size = 40, 4096
	blob := bigBatch(t, n, size)
	cr := &countingReader{r: bytes.NewReader(blob)}

	var readAtFirst, got int
	require.NoError(t, streamBatchResponse(cr, func(e BatchEntry) {
		if got == 0 {
			readAtFirst = cr.n
		}
		got++
		assert.Len(t, e.Data, size)
	}))

	require.Equal(t, n, got)
	// A whole-response reader would sit at len(blob) here. Half is a wide
	// margin around the tar block buffering a streaming reader still does.
	assert.Less(t, readAtFirst, len(blob)/2,
		"the first entry arrived after %d of %d bytes: the response is being buffered whole", readAtFirst, len(blob))
}

// Streaming must not change what the caller ends up with.
func TestStreamBatchResponseMatchesTheCollectedForm(t *testing.T) {
	blob := bigBatch(t, 8, 128)
	entries, err := parseBatchResponse(bytes.NewReader(blob))
	require.NoError(t, err)
	require.Len(t, entries, 8)
	for _, e := range entries {
		assert.Len(t, e.Data, 128)
		assert.Equal(t, int64(128), e.RawSize)
		assert.NotEmpty(t, e.OutputID)
	}
}

// A body the manifest never names is dropped rather than handed over under an
// empty key: the manifest, not the member list, decides what the response says.
func TestStreamBatchResponseDropsAnUnnamedBody(t *testing.T) {
	var m batchGetManifest
	key := fmt.Sprintf("go-buildcache/v1%064x", 1)
	m.Entries = append(m.Entries, batchGetManifestEntry{
		Key:      key,
		Size:     4,
		Metadata: map[string]string{"outputid": "aa", "body-size": "4"},
	})
	blob := buildBatchTar(t, m, map[string][]byte{
		key:                                     []byte("keep"),
		fmt.Sprintf("go-buildcache/v1%064x", 2): []byte("drop"),
	})

	entries, err := parseBatchResponse(bytes.NewReader(blob))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, key, entries[0].Key)
	assert.Equal(t, []byte("keep"), entries[0].Data)
}
