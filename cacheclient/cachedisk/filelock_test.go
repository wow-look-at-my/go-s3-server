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

	"github.com/stretchr/testify/assert"
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

// lockWithin is LockFile bounded by d.
func lockWithin(path string, d time.Duration) (func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return LockFile(ctx, path)
}

// A second holder blocks until the first lets go, then gets the lock.
func TestLockFileExcludesASecondHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	first, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err)

	got := make(chan func())
	go func() {
		unlock, err := lockWithin(path, 10*time.Second)
		assert.Nil(t, err)

		got <- unlock
	}()
	select {
	case <-got:
		t.Fatal("a second holder got the lock while the first held it")
	case <-time.After(200 * time.Millisecond):
	}

	first()
	unlock := <-got
	require.NotNil(t, unlock, "the second holder gets the lock once the first lets go")
	unlock()
}

// A ctx that ends returns the waiter at once, and the abandoned wait does not
// keep the lock once it is granted.
func TestLockFileCancelReturnsAtOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	holder, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		_, err := LockFile(ctx, path)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled wait did not return")
	}

	holder()
	unlock, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err, "the abandoned waiter let go of the lock it was granted")
	unlock()
}

// A holder that dies without unlocking releases the lock: the kernel drops it
// with the process.
func TestLockFileHolderDeathReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	child := startHolder(t, path)

	_, err := lockWithin(path, 200*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the child holds the lock")

	require.NoError(t, child.Process.Kill())
	unlock, err := lockWithin(path, 10*time.Second)
	require.NoError(t, err, "the dead child's lock is released")
	unlock()
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
