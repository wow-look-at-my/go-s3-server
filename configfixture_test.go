package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeConfigFile marshals cfg into a JSON file called name under dir and
// returns the path. A config fixture spelled as source text has to carry a
// temp directory's path, and a quote or a backslash in that path breaks the
// document, so the fixture is a Go value and encoding/json writes it.
func writeConfigFile(t *testing.T, dir, name string, cfg map[string]any) string {
	t.Helper()
	body, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, body, 0o644))
	return path
}

// testCredentials is the one credential every config fixture needs to pass
// validation, as the value writeConfigFile marshals.
func testCredentials(username, password any) []any {
	return []any{map[string]any{"username": username, "password": password}}
}

// envVarRef is a credential field the loader resolves from the environment.
func envVarRef(name string) map[string]any {
	return map[string]any{"type": "envvar", "name": name}
}
