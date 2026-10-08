package cachedisk

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-ipc/filelock"
)

// holderEnv names the lock file a child copy of this test binary takes and holds until it is killed.
const holderEnv = "CACHEDISK_TEST_LOCK_HOLDER"

// TestMain runs the lock holder when this binary was started as one.
func TestMain(m *testing.M) {
	if path := os.Getenv(holderEnv); path != "" {
		holdUntilKilled(path)
	}
	os.Exit(m.Run())
}

// holdUntilKilled takes the lock at path, says so on stdout, and blocks on
// stdin, which the parent never writes or closes.
func holdUntilKilled(path string) {
	if _, err := filelock.Lock(context.Background(), path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("locked")
	io.ReadAll(os.Stdin)
	os.Exit(0)
}

// startHolder starts a child process holding the lock at path and returns
// once the child reports the lock is held.
func startHolder(t *testing.T, path string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), holderEnv+"="+path)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	t.Cleanup(func() { stdin.Close() })
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "locked\n", line)
	return cmd
}

// transformFile serializes against a lock held by another process.
func TestTransformFileWaitsForHolder(t *testing.T) {
	dir := t.TempDir()
	stamp := filepath.Join(dir, "trim.txt")
	child := startHolder(t, stamp+".lock")

	done := make(chan error)
	go func() {
		done <- transformFile(stamp, func([]byte) ([]byte, error) { return []byte("1"), nil })
	}()
	select {
	case err := <-done:
		t.Fatalf("transformFile ran while another process held the lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, child.Process.Kill())
	require.NoError(t, <-done)
	data, err := os.ReadFile(stamp)
	require.NoError(t, err)
	require.Equal(t, "1", string(data))
}
