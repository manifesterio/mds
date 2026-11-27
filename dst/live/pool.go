package live

import "sync"

const defaultExecWorkers = 100

// execPool runs the IO of every actor of a Host. Any worker runs any job;
// there is no order between jobs. The queue has no limit, so queueing never
// blocks an actor.
type execPool struct {
	workers int

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []func()
	busy   int
	closed bool
	wg     sync.WaitGroup
}

func newExecPool(workers int) *execPool {
	p := &execPool{workers: workers}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// start starts the workers.
func (p *execPool) start() {
	for range p.workers {
		p.wg.Go(p.work)
	}
}

// enqueue adds job to the queue and returns at once. After close the job is
// dropped.
func (p *execPool) enqueue(job func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.queue = append(p.queue, job)
	p.cond.Signal()
}

func (p *execPool) work() {
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if len(p.queue) == 0 { // closed and drained
			p.mu.Unlock()
			return
		}
		job := p.queue[0]
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.busy++
		p.mu.Unlock()
		job()
		p.mu.Lock()
		p.busy--
		p.mu.Unlock()
	}
}

// close stops new jobs: the workers run the jobs already queued, then end.
func (p *execPool) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cond.Broadcast()
}

// stats reports the queued and then the running jobs.
func (p *execPool) stats() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue), p.busy
}
