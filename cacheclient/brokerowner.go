// All rights reserved. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !js && !wasip1

package cacheclient

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// A build spreads over many processes.

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

var liveBroker atomic.Pointer[brokerServer]

// brokerMu orders serveBroker against StopBroker and Exit. After Exit sets brokerStopped, no broker starts.
var (
	brokerMu      sync.Mutex
	brokerStopped bool
)

// serveBroker serves owner under a name of this process's own and puts that
// name in the environment. It serves nothing when this process must not
// serve, which is not an error: the cache then works as it did.
func serveBroker(owner Cache, dir string) {
	if os.Getenv(brokerOffEnv) != "" || os.Getenv(BrokerEnv) != "" {
		return
	}
	brokerMu.Lock()
	defer brokerMu.Unlock()
	if brokerStopped {
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
	os.Setenv(BrokerEnv, name)
	liveBroker.Store(bkr)
	brokerNotice("cache: serving the build cache as %s", name)
}

// endpointName answers a shared-memory name no other process holds.
func endpointName(kind string) string {
	return kind + "-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// BrokerEnviron is what a child needs in its environment to find this
// process's cache, or nothing when this process serves none.
func BrokerEnviron() []string {
	if bkr := liveBroker.Load(); bkr != nil {
		return []string{BrokerEnv + "=" + bkr.name}
	}
	return nil
}

// StopBroker releases the name. The consumer calls it as the process exits,
// after every child it started has gone.
func StopBroker() {
	brokerMu.Lock()
	bkr := liveBroker.Swap(nil)
	brokerMu.Unlock()
	if bkr != nil {
		bkr.shutdown()
		// The name is gone, so a child handed this environment afterwards would wait on nobody.
		os.Unsetenv(BrokerEnv)
	}
}

// Exit closes every endpoint this process holds and removes its life socket,
// which no other process removes while the directory lives.
func Exit() error {
	brokerMu.Lock()
	brokerStopped = true
	brokerMu.Unlock()
	StopBroker()
	if child := liveChild.Swap(nil); child != nil {
		child.Close()
	}
	return ipc.Release()
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

// attend answers a single child until it goes away. Each request runs on a
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

// answer handles a single request and sends its reply.
func (bkr *brokerServer) answer(ctx context.Context, child *ipc.Channel, typ uint32, req []byte) {
	id, action, rest, err := readRequest(req)
	if err != nil {
		if at, idErr := recordID(req); idErr == nil {
			child.SendTyped(ctx, stError, getRecord(nil, at, ActionID{}))
		}
		return
	}
	switch typ {
	case opGet, opGetLocal:
		entry, tier, miss := bkr.lookup(action, typ == opGetLocal)
		if miss != nil {
			child.SendTyped(ctx, stMiss, getRecord(nil, id, action))
			return
		}
		child.SendTyped(ctx, stEntry, entryRecord(nil, id, entry, tier))
	case opPut, opPutLocal:
		entry, err := bkr.store(action, string(rest), typ == opPutLocal)
		if err != nil {
			child.SendTyped(ctx, stError, append(getRecord(nil, id, action), err.Error()...))
			return
		}
		child.SendTyped(ctx, stEntry, entryRecord(nil, id, entry, TierDisk))
	default:
		child.SendTyped(ctx, stError, getRecord(nil, id, action))
	}
}

// lookup answers an action. Everyone who asks for that action while the
// earliest ask runs shares its result, and parks until the close wakes them.
//
// A local lookup asks the directory alone, for an output the shared store
// must not see.
func (bkr *brokerServer) lookup(action ActionID, local bool) (Entry, string, error) {
	key := flightKey(action, local)
	flight, mine := bkr.fly.startGet(key)
	if mine {
		func() {
			defer bkr.fly.finishGet(key, flight)
			entry, tier, err := bkr.ownerGet(action, local)
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
// Children that built the same object offer it at the same time. the
// earliest offer is the store that happens, so the body is read and stored a single time.
func (bkr *brokerServer) store(action ActionID, path string, local bool) (Entry, error) {
	owner := bkr.owner
	if local {
		owner = cachedisk.Local(owner)
	}
	key := flightKey(action, local)
	flight, mine := bkr.fly.startPut(key)
	if !mine {
		<-flight.done
		if entry, err := owner.Get(action); err == nil {
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
	out, size, err := owner.Put(action, file)
	if err != nil {
		return Entry{}, err
	}
	return Entry{OutputID: out, Size: size, Time: time.Now()}, nil
}

// flightKey names an action in flight. A local ask and a shared ask of one
// action are different work, so each has its own flight.
func flightKey(action ActionID, local bool) string {
	if local {
		return "local\x00" + string(action[:])
	}
	return string(action[:])
}

func (bkr *brokerServer) ownerGet(id ActionID, local bool) (Entry, string, error) {
	if local {
		entry, err := cachedisk.Local(bkr.owner).Get(id)
		return entry, TierDisk, err
	}
	if tiered, ok := bkr.owner.(Tiered); ok {
		return tiered.GetTiered(id)
	}
	entry, err := bkr.owner.Get(id)
	return entry, TierDisk, err
}

// brokerNotice reports what the broker did, to the installed Logger.
func brokerNotice(format string, args ...any) {
	logging.Infof(format, args...)
}
