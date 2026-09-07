package cacheclient

import (
	"runtime"
	"sync"
)

// putJob is a claimed object waiting for a prep worker.
//
// The body arrives one of two ways. data carries it for a caller that already
// holds the bytes. path names a file for a caller that does not, and then the
// body is not resident while the job waits: the queue is four times the worker
// count deep, so a body held from submit to prepare cost four times the peak a
// body read ON the worker does.
type putJob struct {
	actionID string
	key      string
	hash     actionHash
	outputID string
	data     []byte
	path     string
}

// prepPool compresses outgoing objects.
//
// Compression is the expensive part of storing an object, and it used to run on
// the goroutine that had just finished a compile -- the exact goroutine the
// build wanted back so it could start the next one. A build that stores
// thousands of objects paid for every one of them in build time.
//
// It is CPU work, so the pool is sized to the machine's cores rather than to
// the fetch pools, which are sized to sockets in flight. The queue is bounded:
// past that depth a build is producing objects faster than this machine can
// compress them, and the correct answer is to slow the producer rather than to
// hold an unbounded pile of uncompressed bodies in memory.
type prepPool struct {
	b    *WebBackend
	jobs chan putJob
	stop chan struct{}
	wg   sync.WaitGroup
	// pending counts objects submitted but not yet prepared, so a caller can
	// wait for the decisions Put made asynchronously.
	pending sync.WaitGroup
}

// await blocks until every object submitted so far has been prepared and
// either queued for upload or refused.
func (p *prepPool) await() {
	if p == nil {
		return
	}
	p.pending.Wait()
}

func newPrepPool(b *WebBackend) *prepPool {
	workers := envInt("GO_TOOLCHAIN_CACHE_PREP", runtime.NumCPU())
	if workers < 2 {
		workers = 2
	}
	if workers > 32 {
		workers = 32
	}
	p := &prepPool{
		b:    b,
		jobs: make(chan putJob, workers*4),
		stop: make(chan struct{}),
	}
	p.wg.Add(workers)
	for range workers {
		go p.worker()
	}
	return p
}

// submit queues an object for preparation. It reports false only when the
// backend is closing, which is the caller's cue to drop the key's claim.
//
// It blocks while the queue is full, and that is deliberate: the alternative
// is dropping an object the build just paid to produce, or letting the queue
// grow without limit. The wait is bounded by how long one object takes to
// compress.
func (p *prepPool) submit(j putJob) bool {
	if p == nil {
		return false
	}
	p.pending.Add(1)
	select {
	case p.jobs <- j:
		return true
	case <-p.stop:
		p.pending.Done()
		return false
	}
}

// Close drains the queue and waits for every worker, so an object handed to
// Put before shutdown still reaches the upload coalescer.
func (p *prepPool) Close() {
	if p == nil {
		return
	}
	close(p.stop)
	p.wg.Wait()
}

func (p *prepPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case j := <-p.jobs:
			p.b.prepare(j)
			p.pending.Done()
		case <-p.stop:
			// Whatever is already queued was claimed and must still be shipped;
			// dropping it here would leave the key claimed and never stored.
			for {
				select {
				case j := <-p.jobs:
					p.b.prepare(j)
					p.pending.Done()
				default:
					return
				}
			}
		}
	}
}
