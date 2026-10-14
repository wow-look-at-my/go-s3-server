package cachedisk

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	if _, err := LockFile(context.Background(), path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("locked")
	io.ReadAll(os.Stdin)
	os.Exit(0)
}

// startHolder holds the lock at path and returns the function that lets go.
// The holder is a child process; js and wasip1 start none, so there it is
// this process.
func startHolder(t *testing.T, path string) (release func()) {
	t.Helper()
	if runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		unlock, err := LockFile(context.Background(), path)
		require.NoError(t, err)
		return unlock
	}
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
	return func() { require.NoError(t, cmd.Process.Kill()) }
}

// transformFile serializes against a lock held by another process.
func TestTransformFileWaitsForHolder(t *testing.T) {
	dir := t.TempDir()
	stamp := filepath.Join(dir, "trim.txt")
	release := startHolder(t, stamp+".lock")

	done := make(chan error)
	go func() {
		done <- transformFile(stamp, func([]byte) ([]byte, error) { return []byte("1"), nil })
	}()
	select {
	case err := <-done:
		t.Fatalf("transformFile ran while another process held the lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	release()
	require.NoError(t, <-done)
	data, err := os.ReadFile(stamp)
	require.NoError(t, err)
	require.Equal(t, "1", string(data))
}
