package cacheclient

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// clockWaits stand in for a wakeup; a goroutine that waits parks instead.
var clockWaits = set.Of("runtime.Gosched", "time.Sleep", "time.After", "time.Tick", "time.NewTicker")

// No library file of the client waits by yielding, sleeping or ticking. A
// timer that bounds an event wait is a time.NewTimer or a context deadline.
func TestNoLibraryCodeSleepsOrTicks(t *testing.T) {
	var found []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != "." && (entry.Name() == "build" || entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		found = append(found, clockWaitCalls(t, path)...)
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, found)
}

func clockWaitCalls(t *testing.T, path string) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	require.NoError(t, err)
	imported := map[string]string{}
	for _, spec := range file.Imports {
		importPath := strings.Trim(spec.Path.Value, `"`)
		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imported[name] = importPath
	}
	var found []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		call := imported[pkg.Name] + "." + selector.Sel.Name
		if clockWaits.Contains(call) {
			found = append(found, fset.Position(selector.Pos()).String()+": "+call)
		}
		return true
	})
	return found
}

// The scan reports a ticker it is shown, so an empty result above means something.
func TestClockWaitCallsFindsATicker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "poll.go")
	source := "package poll\n\nimport \"time\"\n\nfunc poll(ready func() bool) {\n\tticker := time.NewTicker(time.Second)\n\tfor range ticker.C {\n\t\tif ready() {\n\t\t\treturn\n\t\t}\n\t}\n}\n"
	require.NoError(t, os.WriteFile(path, []byte(source), 0o644))
	found := clockWaitCalls(t, path)
	require.Len(t, found, 1)
	assert.Contains(t, found[0], "time.NewTicker")
}
