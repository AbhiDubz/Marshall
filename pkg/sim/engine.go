package sim

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Engine replays a trace through a scheduler and reports what
// happened. It is event driven: the virtual clock jumps from event to
// event (submit or finish); after draining all events at a timestamp
// the scheduler runs once and returned allocations start immediately.
type Engine struct {
	trace     *Trace
	scheduler sched.Scheduler

	clock   time.Time
	nodes   []types.Node
	jobs    map[string]*simJob
	pending []string // job IDs, insertion order (sorted per cycle)
	running map[string]*simJob
	events  eventHeap
	seq     int64

	// utilization integral state
	lastAccount time.Time
	allocCPU    int64
	allocMem    int64
	cpuMSAcc    float64 // Σ allocatedCPU(t) dt, in millicore·ms
	memMSAcc    float64

	firstSubmit time.Time
	lastFinish  time.Time
	completed   int
}

type simJob struct {
	job     types.Job
	trueRun time.Duration
	alloc   types.Allocation
	started time.Time
	waited  time.Duration
}

type event struct {
	at   time.Time
	seq  int64 // FIFO tie-break for identical timestamps
	kind string
	job  string
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, k int) bool {
	if !h[i].at.Equal(h[k].at) {
		return h[i].at.Before(h[k].at)
	}
	return h[i].seq < h[k].seq
}
func (h eventHeap) Swap(i, k int)       { h[i], h[k] = h[k], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// NewEngine builds an engine for one run. The scheduler is constructed
// by the caller (it may need the engine's job lookup — see Lookup).
func NewEngine(t *Trace) *Engine {
	e := &Engine{
		trace:   t,
		nodes:   t.Nodes(),
		jobs:    make(map[string]*simJob, len(t.Jobs)),
		running: make(map[string]*simJob),
	}
	for _, tj := range t.Jobs {
		j := tj.Job()
		e.jobs[j.ID] = &simJob{job: j, trueRun: time.Duration(tj.TrueRuntimeMS) * time.Millisecond}
		e.push(event{at: j.SubmitAt, kind: "submit", job: j.ID})
	}
	return e
}

// Lookup is the sched.JobLookup over every job in the trace; backfill
// uses it to project running-job completion from estimates.
func (e *Engine) Lookup(id string) (types.Job, bool) {
	j, ok := e.jobs[id]
	if !ok {
		return types.Job{}, false
	}
	return j.job.Clone(), true
}

// SetScheduler injects the scheduler (after construction so the
// scheduler could be built with e.Lookup).
func (e *Engine) SetScheduler(s sched.Scheduler) { e.scheduler = s }

func (e *Engine) push(ev event) {
	e.seq++
	ev.seq = e.seq
	heap.Push(&e.events, ev)
}

// Run replays the whole trace and returns the result. It errors only
// on internal invariant violations (a scheduler double-booking a node,
// or referencing an unknown job) — bad policy is a bad number in the
// report, not an error.
func (e *Engine) Run() (*Result, error) {
	if e.scheduler == nil {
		return nil, fmt.Errorf("no scheduler set")
	}
	if len(e.events) == 0 {
		return nil, fmt.Errorf("trace has no jobs")
	}
	e.clock = e.events[0].at
	e.firstSubmit = e.events[0].at
	e.lastAccount = e.clock

	for e.events.Len() > 0 {
		now := e.events[0].at
		e.account(now)
		e.clock = now
		for e.events.Len() > 0 && e.events[0].at.Equal(now) {
			ev := heap.Pop(&e.events).(event)
			switch ev.kind {
			case "submit":
				e.pending = append(e.pending, ev.job)
			case "finish":
				if err := e.finish(ev.job, now); err != nil {
					return nil, err
				}
			}
		}
		if err := e.schedule(now); err != nil {
			return nil, err
		}
	}

	unschedulable := len(e.pending)
	return e.result(unschedulable), nil
}

// account integrates allocated resources over [lastAccount, now).
func (e *Engine) account(now time.Time) {
	dt := now.Sub(e.lastAccount)
	if dt > 0 {
		ms := float64(dt.Milliseconds())
		e.cpuMSAcc += float64(e.allocCPU) * ms
		e.memMSAcc += float64(e.allocMem) * ms
	}
	e.lastAccount = now
}

func (e *Engine) finish(jobID string, now time.Time) error {
	sj, ok := e.running[jobID]
	if !ok {
		return fmt.Errorf("finish event for job %s that is not running", jobID)
	}
	for _, nid := range sj.alloc.NodeIDs {
		n := e.node(nid)
		if n == nil {
			return fmt.Errorf("finish: unknown node %s", nid)
		}
		n.Allocated = n.Allocated.Sub(sj.job.Request)
		e.allocCPU -= sj.job.Request.CPUMillis
		e.allocMem -= sj.job.Request.MemoryBytes >> 20 // MiB to keep the integral in float range
	}
	sj.job.State = types.Completed
	delete(e.running, jobID)
	e.completed++
	if now.After(e.lastFinish) {
		e.lastFinish = now
	}
	return nil
}

func (e *Engine) node(id string) *types.Node {
	for i := range e.nodes {
		if e.nodes[i].ID == id {
			return &e.nodes[i]
		}
	}
	return nil
}

func (e *Engine) schedule(now time.Time) error {
	if len(e.pending) == 0 {
		return nil
	}
	// Build the queue sorted by submit time then ID (the contract:
	// "queue is sorted by submit time; the scheduler applies its own
	// priority rules").
	queue := make([]types.Job, 0, len(e.pending))
	for _, id := range e.pending {
		queue = append(queue, e.jobs[id].job.Clone())
	}
	sort.Slice(queue, func(i, k int) bool {
		if !queue[i].SubmitAt.Equal(queue[k].SubmitAt) {
			return queue[i].SubmitAt.Before(queue[k].SubmitAt)
		}
		return queue[i].ID < queue[k].ID
	})

	nodes := make([]types.Node, len(e.nodes))
	for i, n := range e.nodes {
		nodes[i] = n.Clone()
	}
	running := make([]types.Allocation, 0, len(e.running))
	runIDs := make([]string, 0, len(e.running))
	for id := range e.running {
		runIDs = append(runIDs, id)
	}
	sort.Strings(runIDs)
	for _, id := range runIDs {
		running = append(running, e.running[id].alloc.Clone())
	}

	allocs := e.scheduler.Schedule(now, queue, nodes, running)

	for _, a := range allocs {
		sj, ok := e.jobs[a.JobID]
		if !ok {
			return fmt.Errorf("scheduler returned unknown job %s", a.JobID)
		}
		if sj.job.State != types.Pending {
			return fmt.Errorf("scheduler started job %s in state %s", a.JobID, sj.job.State)
		}
		if len(a.NodeIDs) != sj.job.NodeCount {
			return fmt.Errorf("job %s: got %d nodes, wants %d", a.JobID, len(a.NodeIDs), sj.job.NodeCount)
		}
		for _, nid := range a.NodeIDs {
			n := e.node(nid)
			if n == nil {
				return fmt.Errorf("scheduler chose unknown node %s", nid)
			}
			if !n.CanFit(sj.job.Request) {
				return fmt.Errorf("scheduler double-booked node %s for job %s", nid, a.JobID)
			}
			n.Allocated = n.Allocated.Add(sj.job.Request)
			e.allocCPU += sj.job.Request.CPUMillis
			e.allocMem += sj.job.Request.MemoryBytes >> 20
		}
		sj.job.State = types.Running
		sj.alloc = a.Clone()
		sj.started = now
		sj.waited = now.Sub(sj.job.SubmitAt)
		e.running[a.JobID] = sj
		e.removePending(a.JobID)
		e.push(event{at: now.Add(sj.trueRun), kind: "finish", job: a.JobID})
	}
	return nil
}

func (e *Engine) removePending(jobID string) {
	for i, id := range e.pending {
		if id == jobID {
			e.pending = append(e.pending[:i], e.pending[i+1:]...)
			return
		}
	}
}
