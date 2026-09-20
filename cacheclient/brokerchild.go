// All rights reserved. Use of this source code is
// governed by a BSD-style license that can be found
// in the LICENSE file.

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

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// brokerCache is the cache in a process that did not open the directory. It
// holds no DiskCache, writes no index entry and trims nothing. Every read and
// write goes to the owner, and what comes back is the identity of a file the
// owner has already written.
//
// OutputFile still answers a path, because that is what the contract promises
// and what a compiler opens. The layout is the owner's, and this process
// computes the same name from the same directory.
type brokerCache struct {
	link *brokerLink
	dir  string
}

// errBrokerMiss is why a broker answer is not an entry: the owner reported no
// such action, or answered something this process cannot read.
var errBrokerMiss = errors.New("the build cache owner has no such entry")

// A brokerLink is this process's channel to the owner.
//
// A build compiles in parallel, so its cache reads are parallel, and the
// replies come back in whatever order the owner finishes. a single goroutine
// reads them and hands each to the caller that is waiting for its correlation
// ID. Every wait is a park: a sender blocks on its own channel, and the
// reader blocks in the ring.
type brokerLink struct {
	channel *ipc.Channel
	next    atomic.Uint64

	mu      sync.Mutex
	waiting map[uint64]chan reply
	closed  bool

	stop context.CancelFunc
	done chan struct{}
}

// A reply is what the owner answered a single request with.
type reply struct {
	typ   uint32
	entry Entry
	tier  string
	err   error
}

// dialBroker answers the cache this process should use when a live owner is
// named in the environment. A name outlives the process that made it, so the
// owner's earliest record is what decides, and it also reports the directory
// the owner writes into.
func dialBroker() Cache {
	name := os.Getenv(brokerEnv)
	if name == "" || os.Getenv(brokerOffEnv) != "" {
		return nil
	}
	hello, err := ipc.OpenQueue(name)
	if err != nil {
		return nil
	}
	defer hello.Close()

	// The channel exists before the owner hears of it, so the owner's open
	// never races the create.
	mine := endpointName("gobuildcache-child")
	channel, err := ipc.CreateChannel(mine)
	if err != nil {
		return nil
	}
	ctx, stop := context.WithCancel(context.Background())
	if err := hello.SendTyped(ctx, opHello, []byte(mine)); err != nil {
		stop()
		channel.Close()
		channel.Unlink()
		return nil
	}
	typ, rec, err := channel.Recv(ctx)
	if err != nil || typ != stDir || len(rec) == 0 {
		stop()
		channel.Close()
		channel.Unlink()
		return nil
	}
	dir := string(rec)
	link := &brokerLink{
		channel: channel,
		waiting: make(map[uint64]chan reply),
		stop:    stop,
		done:    make(chan struct{}),
	}
	go link.read(ctx)
	brokerNotice("cache: served by the build cache owner at %s", name)
	return &brokerCache{link: link, dir: dir}
}

// read hands each reply to whoever is waiting for it. It parks in the ring
// between replies and wakes when a single lands.
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
	answer, err := c.link.ask(opGet, func(at uint64) []byte {
		return getRecord(nil, at, id)
	})
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

// Put hands a body to the owner by naming the file it sits in. The owner reads
// it before it answers, so this process may exit as soon as Put returns.
//
// A caller that already holds an open file names that file. a single that
// holds anything else spills to a temporary file earliest: the bytes have
// to reach another process, and a path is what this protocol carries.
func (c *brokerCache) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	path, cleanup, err := readerPath(file)
	if err != nil {
		return OutputID{}, 0, err
	}
	defer cleanup()
	answer, err := c.link.ask(opPut, func(at uint64) []byte {
		return putRecord(nil, at, id, path)
	})
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
