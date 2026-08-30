package cacheclient

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func encodeConfig(t *testing.T, raw string) string {
	t.Helper()
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

func TestConfigFromEnv_Unset(t *testing.T) {
	t.Setenv(ConfigEnv, "")
	require.Empty(t, ConfigFromEnv().Bucket, "no configuration means no remote")
}

func TestConfigFromEnv_NativeCredentials(t *testing.T) {
	t.Setenv(ConfigEnv, encodeConfig(t,
		`{"endpoint":"cache.example.com","bucket":"b","username":"u","password":"p"}`))
	cfg := ConfigFromEnv()
	require.Equal(t, "b", cfg.Bucket)
	require.Equal(t, "cache.example.com", cfg.Endpoint)
	require.Equal(t, "u", cfg.AccessKey)
	require.Equal(t, "p", cfg.SecretKey)
}

func TestConfigFromEnv_DefaultBucket(t *testing.T) {
	t.Setenv(ConfigEnv, encodeConfig(t, `{"endpoint":"e","username":"u","password":"p"}`))
	require.Equal(t, DefaultBucket, ConfigFromEnv().Bucket)
}

// The S3-era field names still configure a client, because a consumer's CI
// configuration outlives any one release of this module.
func TestConfigFromEnv_DeprecatedS3Spellings(t *testing.T) {
	t.Setenv(ConfigEnv, encodeConfig(t,
		`{"endpoint":"e","key_id":"u","access_key":"p","region":"us-east-1"}`))
	cfg := ConfigFromEnv()
	require.Equal(t, "u", cfg.AccessKey)
	require.Equal(t, "p", cfg.SecretKey)
}

// URL-safe, unpadded and line-wrapped base64 all reach the same config: the
// value travels through CI systems that re-encode it.
func TestConfigFromEnv_Base64Dialects(t *testing.T) {
	raw := `{"endpoint":"e","username":"u","password":"p"}`
	for name, encoded := range map[string]string{
		"url-safe": base64.URLEncoding.EncodeToString([]byte(raw)),
		"unpadded": base64.RawStdEncoding.EncodeToString([]byte(raw)),
		"wrapped":  base64.StdEncoding.EncodeToString([]byte(raw))[:8] + "\n" + base64.StdEncoding.EncodeToString([]byte(raw))[8:],
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(ConfigEnv, encoded)
			require.Equal(t, "e", ConfigFromEnv().Endpoint)
		})
	}
}

// A configuration that cannot authenticate is no remote, not a broken one: the
// build proceeds against the local cache alone.
func TestConfigFromEnv_RejectsIncomplete(t *testing.T) {
	for name, raw := range map[string]string{
		"no endpoint":    `{"username":"u","password":"p"}`,
		"no credentials": `{"endpoint":"e"}`,
		"no password":    `{"endpoint":"e","username":"u"}`,
		"not json":       `not json at all`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(ConfigEnv, encodeConfig(t, raw))
			require.Empty(t, ConfigFromEnv().Bucket)
		})
	}
}

func TestConfigFromEnv_RejectsGarbageBase64(t *testing.T) {
	t.Setenv(ConfigEnv, "not-valid-base64!!!")
	require.Empty(t, ConfigFromEnv().Bucket)
}
