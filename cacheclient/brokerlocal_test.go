// A child's local requests stop at the owner's directory.

//go:build !js && !wasip1

package cacheclient

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync/atomic"
	"testing"

	ipc "github.com/wow-look-at-my/go-ipc"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// layeredOwner is an owner with a shared tier over its directory. It counts
// every request that reaches the shared tier.
type layeredOwner struct {
	Cache
	sharedGets atomic.Int64
	sharedPuts atomic.Int64
}

func (o *layeredOwner) Get(id ActionID) (Entry, error) {
	o.sharedGets.Add(1)
	return o.Cache.Get(id)
}

func (o *layeredOwner) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	o.sharedPuts.Add(1)
	return o.Cache.Put(id, file)
}

func (o *layeredOwner) Local() Cache { return o.Cache }

// A module index must never reach the shared store, which refuses it. A
// child asks for one through Local, and the owner answers from its directory
// alone: the shared tier sees no lookup and no upload.
func TestBrokerChildLocalStopsAtTheOwnersDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(BrokerEnv, "")
	t.Setenv(brokerOffEnv, "")
	disk, err := Open(dir)
	require.NoError(t, err)
	owner := &layeredOwner{Cache: disk}
	serveBroker(owner, dir)
	t.Cleanup(StopBroker)
	child := dialBroker(dir)
	require.NotNil(t, child)
	t.Cleanup(func() { child.Close() })

	local := cachedisk.Local(child)
	require.NotEqual(t, child, local, "a child must answer a local cache of its own")

	id := ActionID(dummyID(23))
	_, err = local.Get(id)
	var miss *MissError
	require.ErrorAs(t, err, &miss, "a local miss is the miss every caller branches on")

	body := []byte("an index only this machine can use")
	out, size, err := local.Put(id, bytes.NewReader(body))
	require.NoError(t, err)
	require.EqualValues(t, len(body), size)

	entry, err := local.Get(id)
	require.NoError(t, err)
	assert.Equal(t, out, entry.OutputID)
	stored, err := os.ReadFile(local.OutputFile(entry.OutputID))
	require.NoError(t, err)
	assert.Equal(t, body, stored)
	assert.Zero(t, owner.sharedGets.Load(), "a local lookup must not reach the shared tier")
	assert.Zero(t, owner.sharedPuts.Load(), "a local store must not reach the shared tier")

	_, err = child.Get(id)
	require.NoError(t, err)
	assert.EqualValues(t, 1, owner.sharedGets.Load(), "the child's own lookup still asks the shared tier")
	require.NoError(t, local.Close(), "closing the local view leaves the channel open")
	_, err = child.Get(id)
	require.NoError(t, err)
}

// serveOldOwner serves dir as an owner older than the local requests does.
// It refuses an opcode it does not know, with no message.
func serveOldOwner(t *testing.T, dir string) Cache {
	t.Helper()
	disk, err := Open(dir)
	require.NoError(t, err)
	name := endpointName("gobuildcache-old")
	hello, err := ipc.CreateQueue(name)
	require.NoError(t, err)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, rec, err := hello.Recv(ctx)
		if err != nil {
			return
		}
		channel, err := ipc.OpenChannel(string(rec))
		if err != nil {
			return
		}
		defer channel.Close()
		bkr := &brokerServer{owner: disk, dir: dir, fly: newFlights()}
		if channel.SendTyped(ctx, stDir, []byte(dir)) != nil {
			return
		}
		for {
			typ, rec, err := channel.Recv(ctx)
			if err != nil {
				return
			}
			req := append([]byte(nil), rec...)
			if typ > opPut {
				at, action, _, _ := readRequest(req)
				channel.SendTyped(ctx, stError, getRecord(nil, at, action))
				continue
			}
			bkr.answer(ctx, channel, typ, req)
		}
	}()
	t.Cleanup(func() {
		stop()
		<-done
		hello.Close()
		hello.Unlink()
	})
	t.Setenv(brokerOffEnv, "")
	t.Setenv(BrokerEnv, name)
	return disk
}

// An owner older than the local requests refuses them. The child then sends
// the request the owner knows. The build works as it did before. It does
// not ask the local form again.
func TestBrokerChildLocalFallsBackForAnOlderOwner(t *testing.T) {
	dir := t.TempDir()
	disk := serveOldOwner(t, dir)
	child := dialBroker(dir)
	require.NotNil(t, child)
	t.Cleanup(func() { child.Close() })
	local := cachedisk.Local(child)

	id := ActionID(dummyID(29))
	_, err := local.Get(id)
	var miss *MissError
	require.ErrorAs(t, err, &miss, "the refusal of an unknown request must not read as a failed lookup")
	assert.True(t, child.(*brokerCache).link.noLocal.Load(), "one refusal is enough to stop asking")

	body := []byte("stored the way the old owner stores")
	_, _, err = local.Put(id, bytes.NewReader(body))
	require.NoError(t, err)
	entry, err := local.Get(id)
	require.NoError(t, err)
	assert.EqualValues(t, len(body), entry.Size)
	_, err = disk.Get(id)
	require.NoError(t, err, "the owner's directory holds what the child stored")
}

// A store the owner fails carries the failure's text. It is not read as an
// unknown request. The child does not send it again in another form.
func TestBrokerChildLocalStoreFailureIsNotAFallback(t *testing.T) {
	dir := t.TempDir()
	_, child := serveOwner(t, dir)
	local := cachedisk.Local(child)

	broker := child.(*brokerCache)
	answer, err := broker.link.ask(opPutLocal, func(at uint64) []byte {
		return putRecord(nil, at, ActionID(dummyID(31)), dir+"/no-such-body")
	})
	require.Error(t, err)
	assert.False(t, answer.unknown, "a failed store names its failure")
	_, retry := broker.sharedOp(opPutLocal, answer)
	assert.False(t, retry)
	assert.False(t, broker.link.noLocal.Load())
	_, err = local.Get(ActionID(dummyID(31)))
	require.Error(t, err)
}
