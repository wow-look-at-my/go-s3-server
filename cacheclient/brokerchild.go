// All rights reserved. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !js && !wasip1

package cacheclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// brokerCache is the cache in a process that did not open the directory. It
// holds no DiskCache, writes no index entry and trims nothing.
type brokerCache struct {
	link *brokerLink
	dir  string
}

// errBrokerMiss is why a broker answer is not an entry: the owner reported no such action.
var errBrokerMiss = errors.New("the build cache owner has no such entry")

// A brokerLink is this process's channel to the owner.
type brokerLink struct {
	channel *ipc.Channel
	next    atomic.Uint64

	mu      sync.Mutex
	waiting map[uint64]chan reply
	closed  bool

	stop context.CancelFunc
	done chan struct{}

	// noLocal is set when the owner refused a local request it does not know.
	noLocal atomic.Bool
}

// A reply is what the owner answered a single request with.
type reply struct {
	typ   uint32
	entry Entry
	tier  string
	err   error
	// unknown marks a refusal that carries no message: the owner does not know the request's opcode.
	unknown bool
}

// dialBroker answers the cache this process should use when a live owner is
// named in the environment. A name outlives the process that made it, so what
// decides is the owner's reply, which also reports the directory it writes
// into. An owner of another directory is refused: this process was asked for
// dir, and a command that sets its own GOCACHE must get that cache.
func dialBroker(dir string) Cache {
	name := os.Getenv(BrokerEnv)
	if name == "" || os.Getenv(brokerOffEnv) != "" {
		return nil
	}
	hello, err := ipc.OpenQueue(name)
	if err != nil {
		return nil
	}
	defer hello.Close()

	// The channel exists before the owner hears of it, so the owner's open never races the create.
	mine := endpointName("gobuildcache-child")
	channel, err := ipc.CreateChannel(mine)
	if err != nil {
		return nil
	}
	ctx, stop := context.WithCancel(context.Background())
	// A queue can outlive its owner. Then nobody answers the hello, so the handshake has a deadline.
	shake, shakeDone := context.WithTimeout(ctx, brokerDialTimeout)
	defer shakeDone()
	if err := hello.SendTyped(shake, opHello, []byte(mine)); err != nil {
		stop()
		channel.Close()
		channel.Unlink()
		brokerDialFailed(name, err)
		return nil
	}
	typ, rec, err := channel.Recv(shake)
	if err != nil || typ != stDir || len(rec) == 0 || filepath.Clean(string(rec)) != filepath.Clean(dir) {
		stop()
		channel.Close()
		channel.Unlink()
		if err != nil {
			brokerDialFailed(name, err)
		}
		return nil
	}
	link := &brokerLink{
		channel: channel,
		waiting: make(map[uint64]chan reply),
		stop:    stop,
		done:    make(chan struct{}),
	}
	go link.read(ctx)
	brokerNotice("cache: served by the build cache owner at %s", name)
	child := &brokerCache{link: link, dir: dir}
	liveChild.Store(child)
	return child
}

// brokerDialTimeout bounds the wait for the owner's first answer.
var brokerDialTimeout = 5 * time.Second

// brokerDialFailed reports that this process opens the cache itself because the named owner did not answer.
func brokerDialFailed(name string, err error) {
	brokerNotice("cache: the build cache owner at %s did not answer (%v), so this process opens the cache itself", name, err)
}

// liveChild is this process's open link to an owner, which Exit closes.
var liveChild atomic.Pointer[brokerCache]

// read hands each reply to whoever is waiting for it. It parks in the ring
// between replies, and the arrival of the next reply wakes it.
func (link *brokerLink) read(ctx context.Context) {
	defer close(link.done)
	for {
		typ, rec, err := link.channel.Recv(ctx)
		if err != nil {
			link.fail(err)
			return
		}
		id, idErr := recordID(rec)
		if idErr != nil {
			continue
		}
		answer := reply{typ: typ}
		switch typ {
		case stEntry:
			answer.entry, answer.tier, answer.err = readEntryRecord(rec)
		case stMiss:
			answer.err = cachedisk.Miss(errBrokerMiss)
		default:
			answer.err = fmt.Errorf("cache: the build cache owner refused the request")
			// A failed request carries the failure's text after the action. An unknown opcode carries none.
			answer.unknown = len(rec) == idBytes+cachedisk.HashSize
		}
		link.deliver(id, answer)
	}
}

// deliver wakes the caller waiting on a correlation ID. A reply nobody waits
// for is dropped: its caller gave up, and the channel it left has no reader.
func (link *brokerLink) deliver(id uint64, answer reply) {
	link.mu.Lock()
	waiter, ok := link.waiting[id]
	delete(link.waiting, id)
	link.mu.Unlock()
	if ok {
		waiter <- answer
	}
}

// fail wakes every caller when the owner goes away. Without it each waits for
// a reply that cannot arrive.
func (link *brokerLink) fail(err error) {
	link.mu.Lock()
	waiting := link.waiting
	link.waiting = make(map[uint64]chan reply)
	link.closed = true
	link.mu.Unlock()
	for _, waiter := range waiting {
		waiter <- reply{err: err}
	}
}

// ask sends a single request and waits for its reply.
func (link *brokerLink) ask(typ uint32, build func(id uint64) []byte) (reply, error) {
	id := link.next.Add(1)
	waiter := make(chan reply, 1)
	link.mu.Lock()
	if link.closed {
		link.mu.Unlock()
		return reply{}, errBrokerMiss
	}
	link.waiting[id] = waiter
	link.mu.Unlock()

	ctx := context.Background()
	if err := link.channel.SendTyped(ctx, typ, build(id)); err != nil {
		link.mu.Lock()
		delete(link.waiting, id)
		link.mu.Unlock()
		return reply{}, err
	}
	answer := <-waiter
	return answer, answer.err
}

// Get asks the owner for an action's output. A miss answers the error the
// contract names, exactly as a local miss does.
func (c *brokerCache) Get(id ActionID) (Entry, error) {
	entry, _, err := c.GetTiered(id)
	return entry, err
}

// GetTiered is Get, naming the tier the owner answered from.
func (c *brokerCache) GetTiered(id ActionID) (Entry, string, error) {
	return c.get(opGet, id)
}

// Local answers the owner's directory alone, for an output the shared store must not see.
func (c *brokerCache) Local() Cache { return brokerLocal{c} }

// brokerLocal is a brokerCache whose requests stop at the owner's directory.
type brokerLocal struct{ *brokerCache }

func (l brokerLocal) Get(id ActionID) (Entry, error) {
	entry, _, err := l.GetTiered(id)
	return entry, err
}

func (l brokerLocal) GetTiered(id ActionID) (Entry, string, error) {
	return l.get(l.localOp(opGetLocal, opGet), id)
}

func (l brokerLocal) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	return l.put(l.localOp(opPutLocal, opPut), id, file)
}

// Close leaves the channel open: it belongs to the brokerCache.
func (l brokerLocal) Close() error { return nil }

// localOp answers local, or shared once the owner has refused a local request.
func (c *brokerCache) localOp(local, shared uint32) uint32 {
	if c.link.noLocal.Load() {
		return shared
	}
	return local
}

// sharedOp answers the shared form of a local opcode the owner did not know.
// An owner older than the local requests answers the shared form, as it did
// before a child had a local one to send.
func (c *brokerCache) sharedOp(typ uint32, answer reply) (uint32, bool) {
	if !answer.unknown {
		return typ, false
	}
	switch typ {
	case opGetLocal:
		c.link.noLocal.Store(true)
		return opGet, true
	case opPutLocal:
		c.link.noLocal.Store(true)
		return opPut, true
	}
	return typ, false
}

func (c *brokerCache) get(typ uint32, id ActionID) (Entry, string, error) {
	answer, err := c.link.ask(typ, func(at uint64) []byte {
		return getRecord(nil, at, id)
	})
	if shared, retry := c.sharedOp(typ, answer); err != nil && retry {
		return c.get(shared, id)
	}
	if err != nil {
		tier := answer.tier
		if tier == "" {
			tier = TierOwner
		}
		if answer.typ == stMiss {
			return Entry{}, tier, err
		}
		return Entry{}, tier, cachedisk.Miss(err)
	}
	tier := answer.tier
	if tier == "" {
		tier = TierOwner
	}
	return answer.entry, tier, nil
}

// Put hands a body to the owner by naming the file it sits in.
func (c *brokerCache) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	return c.put(opPut, id, file)
}

func (c *brokerCache) put(typ uint32, id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	path, cleanup, err := readerPath(file)
	if err != nil {
		return OutputID{}, 0, err
	}
	defer cleanup()
	answer, err := c.link.ask(typ, func(at uint64) []byte {
		return putRecord(nil, at, id, path)
	})
	if shared, retry := c.sharedOp(typ, answer); err != nil && retry {
		answer, err = c.link.ask(shared, func(at uint64) []byte {
			return putRecord(nil, at, id, path)
		})
	}
	if err != nil {
		return OutputID{}, 0, err
	}
	return answer.entry.OutputID, answer.entry.Size, nil
}

// OutputFile answers where the owner stored an output. The layout belongs to
// the directory, and this is the same name it writes.
func (c *brokerCache) OutputFile(out OutputID) string {
	return filepath.Join(c.dir, fmt.Sprintf("%02x", out[0]), fmt.Sprintf("%x", out)+"-d")
}

// FuzzDir is the owner's, like everything else in the directory.
func (c *brokerCache) FuzzDir() string {
	return filepath.Join(c.dir, "fuzz")
}

// Close drops the channel. Nothing here owns a file, an upload or a trim, so
// there is nothing to wait for.
func (c *brokerCache) Close() error {
	liveChild.CompareAndSwap(c, nil)
	c.link.stop()
	<-c.link.done
	err := c.link.channel.Close()
	c.link.channel.Unlink()
	return err
}

// readerPath names a file holding what the reader holds. An open file names
// itself; anything else is copied to a temporary file, which cleanup removes.
func readerPath(file io.ReadSeeker) (path string, cleanup func(), err error) {
	if open, ok := file.(*os.File); ok {
		if _, err := open.Seek(0, io.SeekStart); err != nil {
			return "", func() {}, err
		}
		return open.Name(), func() {}, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", func() {}, err
	}
	temp, err := os.CreateTemp("", "gocacheput-")
	if err != nil {
		return "", func() {}, err
	}
	remove := func() {
		temp.Close()
		os.Remove(temp.Name())
	}
	if _, err := io.Copy(temp, file); err != nil {
		remove()
		return "", func() {}, err
	}
	if err := temp.Close(); err != nil {
		os.Remove(temp.Name())
		return "", func() {}, err
	}
	return temp.Name(), func() { os.Remove(temp.Name()) }, nil
}
