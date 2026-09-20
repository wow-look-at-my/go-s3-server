// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// A build is not one process, and a cache each of its processes opens for
// itself costs each of them an index write, a trim, a connection to the store,
// and an exit held open to drain uploads. One process owns the cache instead,
// and serves the rest over shared memory.
//
// That leaves one writer for the directory, so the trim has a single owner.
// Nothing spins: a send into a ring with room makes no system call, and a side
// with nothing to do parks on a kernel wait rather than looking again.

// brokerEnv names the owner's hello queue. A process that finds it set and
// answering is a child. A process that finds it unset becomes the owner.
const brokerEnv = "GO_BUILDCACHE_BROKER"

// brokerOffEnv makes every process open the cache for itself, which is what a
// bisect of a broker-shaped problem wants.
const brokerOffEnv = "GO_BUILDCACHE_BROKER_OFF"

// brokerServer serves the owner's cache to the processes below it.
type brokerServer struct {
	owner Cache
	dir   string
	name  string
	hello *ipc.Queue
	stop  context.CancelFunc
	done  chan struct{}
	once  sync.Once
	fly   *flights
	serve sync.WaitGroup
}

// liveBroker is the server this process runs, if it runs one.
var liveBroker atomic.Pointer[brokerServer]

// serveBroker serves owner under a name of this process's own and puts that
// name in the environment. It serves nothing when this process must not
// serve, which is not an error: the cache then works as it did.
func serveBroker(owner Cache, dir string) {
	if os.Getenv(brokerOffEnv) != "" || os.Getenv(brokerEnv) != "" {
		return
	}
	name := endpointName("gobuildcache")
	hello, err := ipc.CreateQueue(name)
	if err != nil {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	bkr := &brokerServer{
		owner: owner,
		dir:   dir,
		name:  name,
		hello: hello,
		stop:  stop,
		done:  make(chan struct{}),
		fly:   newFlights(),
	}
	go bkr.greet(ctx)
	os.Setenv(brokerEnv, name)
	liveBroker.Store(bkr)
	brokerNotice("cache: serving the build cache as %s", name)
}

// endpointName answers a shared-memory name no other process holds.
func endpointName(kind string) string {
	return kind + "-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// BrokerEnviron is what a child needs in its environment to find this
// process's cache, or nothing when this process serves none.
//
// A consumer that starts its children with os.Environ needs none of this. One
// that builds a curated environment, as the go command does for a test binary,
// adds these entries to it.
func BrokerEnviron() []string {
	if bkr := liveBroker.Load(); bkr != nil {
		return []string{brokerEnv + "=" + bkr.name}
	}
	return nil
}

// StopBroker releases the name. The consumer calls it as the process exits,
// after every child it started has gone.
func StopBroker() {
	if bkr := liveBroker.Swap(nil); bkr != nil {
		bkr.shutdown()
		// The name is gone, so a child handed this environment afterwards
		// would wait on nobody.
		os.Unsetenv(brokerEnv)
	}
}

// shutdown ends the greeting, waits for the per-child servers, and unlinks the
// name. Every child was started by this process and has exited.
func (bkr *brokerServer) shutdown() {
	bkr.once.Do(func() {
		bkr.stop()
		bkr.hello.Close()
		<-bkr.done
		bkr.serve.Wait()
		bkr.hello.Unlink()
	})
}

// greet reads the name each child created and starts serving it. A child
// creates its channel BEFORE it says hello, so the open below always finds it.
func (bkr *brokerServer) greet(ctx context.Context) {
	defer close(bkr.done)
	for {
		typ, rec, err := bkr.hello.Recv(ctx)
		if err != nil {
			return
		}
		if typ != opHello || len(rec) == 0 {
			continue
		}
		child, err := ipc.OpenChannel(string(rec))
		if err != nil {
			continue
		}
		bkr.serve.Add(1)
		go func() {
			defer bkr.serve.Done()
			defer child.Close()
			bkr.attend(ctx, child)
		}()
	}
}

// attend answers one child until it goes away. Each request runs on a
// goroutine of its own: a lookup that reaches the store must not hold up the
// ones behind it.
func (bkr *brokerServer) attend(ctx context.Context, child *ipc.Channel) {
	if err := child.SendTyped(ctx, stDir, []byte(bkr.dir)); err != nil {
		return
	}
	var work sync.WaitGroup
	defer work.Wait()
	for {
		typ, rec, err := child.Recv(ctx)
		if err != nil {
			return
		}
		// Recv hands back the ring's own bytes, which the next read reuses.
		req := append([]byte(nil), rec...)
		work.Add(1)
		go func() {
			defer work.Done()
			bkr.answer(ctx, child, typ, req)
		}()
	}
}

// answer handles one request and sends its reply.
func (bkr *brokerServer) answer(ctx context.Context, child *ipc.Channel, typ uint32, req []byte) {
	id, action, rest, err := readRequest(req)
	if err != nil {
		if at, idErr := recordID(req); idErr == nil {
			child.SendTyped(ctx, stError, getRecord(nil, at, ActionID{}))
		}
		return
	}
	switch typ {
	case opGet:
		entry, tier, miss := bkr.lookup(action)
		if miss != nil {
			child.SendTyped(ctx, stMiss, getRecord(nil, id, action))
			return
		}
		child.SendTyped(ctx, stEntry, entryRecord(nil, id, entry, tier))
	case opPut:
		entry, err := bkr.store(action, string(rest))
		if err != nil {
			child.SendTyped(ctx, stError, append(getRecord(nil, id, action), err.Error()...))
			return
		}
		child.SendTyped(ctx, stEntry, entryRecord(nil, id, entry, TierDisk))
	default:
		child.SendTyped(ctx, stError, getRecord(nil, id, action))
	}
}

// lookup answers an action, sharing the first asker's work with everyone who
// asks for the same one while it runs. The rest park until the close wakes
// them.
func (bkr *brokerServer) lookup(action ActionID) (Entry, string, error) {
	key := string(action[:])
	flight, mine := bkr.fly.startGet(key)
	if mine {
		func() {
			defer bkr.fly.finishGet(key, flight)
			entry, tier, err := bkr.ownerGet(action)
			flight.entry, flight.tier, flight.miss = entry, tier, err != nil
		}()
	} else {
		<-flight.done
	}
	if flight.miss {
		return Entry{}, flight.tier, errBrokerMiss
	}
	return flight.entry, flight.tier, nil
}

// store reads the body the child named by path, before it answers, so the
// child may exit as soon as its Put returns.
//
// Two children that built the same object offer it at once. The first one's
// store is the one that happens, so one body is read and stored once.
func (bkr *brokerServer) store(action ActionID, path string) (Entry, error) {
	key := string(action[:])
	flight, mine := bkr.fly.startPut(key)
	if !mine {
		<-flight.done
		if entry, err := bkr.owner.Get(action); err == nil {
			return entry, nil
		}
	} else {
		defer bkr.fly.finishPut(key, flight)
	}
	if path == "" {
		return Entry{}, errShortRecord
	}
	file, err := os.Open(path)
	if err != nil {
		return Entry{}, err
	}
	defer file.Close()
	out, size, err := bkr.owner.Put(action, file)
	if err != nil {
		return Entry{}, err
	}
	return Entry{OutputID: out, Size: size, Time: time.Now()}, nil
}

// ownerGet looks an action up in the owner's cache, naming the tier that
// answered when the owner can report one.
func (bkr *brokerServer) ownerGet(id ActionID) (Entry, string, error) {
	if tiered, ok := bkr.owner.(Tiered); ok {
		return tiered.GetTiered(id)
	}
	entry, err := bkr.owner.Get(id)
	return entry, TierDisk, err
}

// brokerNotice reports what the broker did, under the variable that makes the
// cache report itself.
func brokerNotice(format string, args ...any) {
	if !cacheDebug() {
		return
	}
	fmt.Fprintf(os.Stderr, "go: "+format+"\n", args...)
}
