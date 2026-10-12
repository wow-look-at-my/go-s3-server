// All rights reserved. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// exitRoleEnv makes the test binary the process that calls Exit, over the cache directory it names.
const exitRoleEnv = "CACHECLIENT_TEST_EXIT_DIR"

// exitThenWait opens the cache as the owner, creates a queue, calls Exit, and
// prints the queue's name. It then lives until its stdin closes, so the
// process that started it can look at it while it runs.
func exitThenWait(dir string) {
	os.Unsetenv(BrokerEnv)
	if _, err := OpenCache(dir, nil); err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	name := endpointName("exit-probe")
	queue, err := ipc.CreateQueue(name)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(2)
	}
	if err := Exit(); err != nil {
		fmt.Fprintln(os.Stderr, "exit:", err)
		os.Exit(2)
	}
	fmt.Println(name)
	io.Copy(io.Discard, os.Stdin)
	queue.Close()
	queue.Unlink()
	os.Exit(0)
}

// Exit removes the process's life socket, so the process leaves nothing in the
// runtime directory. A queue it still holds then reads as one whose creator is
// gone, while that process runs.
func TestExitReleasesTheLifeSocket(t *testing.T) {
	self, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(self, "-test.run=^$")
	cmd.Env = append(os.Environ(), exitRoleEnv+"="+t.TempDir(), BrokerEnv+"=")
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	defer func() {
		stdin.Close()
		require.NoError(t, cmd.Wait())
	}()

	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	name := strings.TrimSpace(line)

	_, err = ipc.OpenQueue(name)
	require.ErrorIs(t, err, ipc.ErrPeerGone, "a released process must read as gone while it still runs")
}
