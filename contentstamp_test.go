package main

import (
	"errors"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stampedArchive is a Go archive whose export header carries the build id
// action/content, zstd-compressed the way the client stores it.
func stampedArchive(t *testing.T, action, content string) []byte {
	t.Helper()
	body := "go object linux amd64 go1.27.0\nbuild id \"" + action + "/" + content + "\"\n\n$$B\nu\n$$\n"
	header := []byte("__.PKGDEF       0           0     0     644     ")
	size := []byte("0         `\n")
	copy(size, []byte(itoa(len(body))))
	raw := append([]byte("!<arch>\n"), header...)
	raw = append(raw, size...)
	raw = append(raw, body...)
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer enc.Close()
	return enc.EncodeAll(raw, nil)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for ; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}

// A key that holds a content-stamped archive is unusable for a client older
// than the stamp. That client's upload of the same object, action-stamped,
// takes the key under write_once deny. Any other difference still conflicts.
func TestWriteOnceLetsAnActionStampReplaceAContentStamp(t *testing.T) {
	storage, err := NewStorage(t.TempDir(), WriteOnceConfig{Action: "deny", Notification: "content_differs"})
	require.NoError(t, err)
	key := cacheprogKey('a')
	contentStamped := stampedArchive(t, "C0ntentHashOfTheObj", "C0ntentHashOfTheObj")
	require.NoError(t, storage.Put(key, contentStamped, map[string]string{"outputid": "x"}, nil))

	before := testutil.ToFloat64(contentStampRepairsTotal)
	actionStamped := stampedArchive(t, "Act10nIdOfTheKey000", "C0ntentHashOfTheObj")
	require.NoError(t, storage.Put(key, actionStamped, map[string]string{"outputid": "y"}, nil), "the same object with its action stamp takes the key")
	assert.Equal(t, 1.0, testutil.ToFloat64(contentStampRepairsTotal)-before)
	stored, err := os.ReadFile(storage.keyToPath(key))
	require.NoError(t, err)
	assert.Equal(t, actionStamped, stored)

	other := stampedArchive(t, "Act10nIdOfTheKey000", "An0therObjectHash00")
	assert.True(t, errors.Is(storage.Put(key, other, nil, nil), ErrWriteOnceConflict), "another object never replaces an action-stamped one")

	key2 := cacheprogKey('b')
	require.NoError(t, storage.Put(key2, actionStamped, nil, nil))
	assert.True(t, errors.Is(storage.Put(key2, contentStamped, nil, nil), ErrWriteOnceConflict), "a content stamp never replaces an action stamp")

	key3 := cacheprogKey('c')
	require.NoError(t, storage.Put(key3, contentStamped, nil, nil))
	assert.True(t, errors.Is(storage.Put(key3, stampedArchive(t, "Act10nIdOfTheKey000", "An0therObjectHash00"), nil, nil), ErrWriteOnceConflict),
		"an action-stamped archive of other content does not replace a content stamp")
	assert.True(t, errors.Is(storage.Put(key3, []byte("not an archive"), nil, nil), ErrWriteOnceConflict))
}

func TestParseArchiveStamp(t *testing.T) {
	action, content, ok := parseArchiveStamp([]byte("!<arch>\n__.PKGDEF ...\ngo object x\nbuild id \"aa/bb\"\n$$\n"))
	require.True(t, ok)
	assert.Equal(t, "aa", action)
	assert.Equal(t, "bb", content)
	_, _, ok = parseArchiveStamp([]byte("not an archive build id \"aa/bb\""))
	assert.False(t, ok)
	_, _, ok = parseArchiveStamp([]byte("!<arch>\n$$\nbuild id \"aa/bb\""))
	assert.False(t, ok, "a build id after the export data is not the header's")
}
