package chaos

import (
	"context"
	"fmt"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Violation is an invariant breach: the seed, tick, and what broke.
type Violation struct {
	Seed      int64
	Tick      int
	Invariant string
	Detail    string
}

func (v *Violation) Error() string {
	return fmt.Sprintf("seed %d tick %d: INVARIANT %s: %s", v.Seed, v.Tick, v.Invariant, v.Detail)
}

// Checker continuously asserts the safety and liveness invariants:
//
//	no-double-run     a job is never accepted as completed twice, and
//	                  no execution exists for an attempt newer than
//	                  the store's, or duplicated within an attempt
//	no-double-booking real node usage and the controller's books both
//	                  stay within capacity, and the books reconcile
//	                  with the store exactly
//	no-job-lost       by the end of the horizon every job is COMPLETED
//	no-starvation     no PENDING job waits longer than the configured
//	                  bound since its (re)enqueue
//	legal-transitions every store write goes through the state machine
//	                  (enforced by MemStore; any breach errors the run)
type Checker struct {
	seed       int64
	accepted   map[string]int
	enqueuedAt map[string]int // jobID -> tick of last (re)enqueue
	startedAt  map[string]int
}

// NewChecker wires a checker to a world.
func NewChecker(w *World) *Checker {
	c := &Checker{
		accepted:   make(map[string]int),
		enqueuedAt: make(map[string]int),
		startedAt:  make(map[string]int),
	}
	for id, cj := range w.jobs {
		c.enqueuedAt[id] = cj.submit
	}
	return c
}

func (c *Checker) acceptedCompletion(jobID string)    { c.accepted[jobID]++ }
func (c *Checker) jobStarted(jobID string, tick int)  { c.startedAt[jobID] = tick }
func (c *Checker) jobRequeued(jobID string, tick int) { c.enqueuedAt[jobID] = tick }

func (c *Checker) violation(w *World, invariant, format string, args ...any) error {
	return &Violation{Seed: c.seed, Tick: w.tick, Invariant: invariant,
		Detail: fmt.Sprintf(format, args...)}
}

// Check runs the continuous invariants for the current tick.
func (c *Checker) Check(w *World) error {
	ctx := context.Background()

	// no-double-run (acceptance side).
	for job, n := range c.accepted {
		if n > 1 {
			return c.violation(w, "no-double-run", "job %s accepted as completed %d times", job, n)
		}
	}

	// no-double-run (execution side) + ground-truth booking.
	type shardKey struct {
		job     string
		attempt int
	}
	shards := make(map[shardKey]int)
	for _, id := range w.agentIDs() {
		a := w.agents[id]
		var used types.ResourceSpec
		for _, token := range sortedTokens(a.execs) {
			ex := a.execs[token]
			if !ex.done {
				used = used.Add(ex.req)
			}
			j, err := w.ctl.st.GetJob(ctx, ex.jobID)
			if err != nil {
				return c.violation(w, "no-job-lost", "execution for unknown job %s", ex.jobID)
			}
			if ex.attempt > j.Attempt {
				return c.violation(w, "no-double-run",
					"node %s runs %s attempt %d but the store only knows attempt %d",
					id, ex.jobID, ex.attempt, j.Attempt)
			}
			if ex.attempt == j.Attempt {
				shards[shardKey{ex.jobID, ex.attempt}]++
			}
		}
		if !used.Fits(a.capacity) {
			return c.violation(w, "no-double-booking",
				"node %s ground-truth usage %+v exceeds capacity %+v", id, used, a.capacity)
		}
	}
	for key, n := range shards {
		j, err := w.ctl.st.GetJob(ctx, key.job)
		if err != nil {
			continue
		}
		if n > j.NodeCount {
			return c.violation(w, "no-double-run",
				"job %s attempt %d has %d live shards, NodeCount=%d", key.job, key.attempt, n, j.NodeCount)
		}
	}

	// Controller bookkeeping: views reconcile with the store exactly.
	expect := make(map[string]types.ResourceSpec)
	running, err := w.ctl.st.ListJobs(ctx, types.Running)
	if err != nil {
		return err
	}
	for _, j := range running {
		a, ok := w.ctl.allocs[j.ID]
		if !ok {
			return c.violation(w, "no-double-booking", "RUNNING job %s has no allocation", j.ID)
		}
		for _, nid := range a.NodeIDs {
			expect[nid] = expect[nid].Add(j.Request)
		}
	}
	for _, id := range w.agentIDs() {
		v := w.ctl.views[id]
		want := expect[id]
		if v.state != "alive" {
			want = types.ResourceSpec{}
		}
		if v.allocated.CPUMillis != want.CPUMillis || v.allocated.MemoryBytes != want.MemoryBytes {
			return c.violation(w, "no-double-booking",
				"node %s books %+v but RUNNING jobs sum to %+v", id, v.allocated, want)
		}
		if !v.allocated.Fits(w.agents[id].capacity) {
			return c.violation(w, "no-double-booking",
				"node %s booked %+v beyond capacity", id, v.allocated)
		}
	}

	// no-starvation: PENDING jobs must not wait beyond the bound
	// since their last (re)enqueue.
	pending, err := w.ctl.st.ListJobs(ctx, types.Pending)
	if err != nil {
		return err
	}
	boundTicks := int(w.cfg.StartBound / Tick)
	for _, j := range pending {
		if w.tick-c.enqueuedAt[j.ID] > boundTicks {
			return c.violation(w, "no-starvation",
				"job %s pending for %v (bound %v)",
				j.ID, time.Duration(w.tick-c.enqueuedAt[j.ID])*Tick, w.cfg.StartBound)
		}
	}
	return nil
}

// Final asserts the end-of-run invariants: nothing lost, everything
// completed.
func (c *Checker) Final(w *World) error {
	ctx := context.Background()
	all, err := w.ctl.st.ListJobs(ctx)
	if err != nil {
		return err
	}
	if len(all) != len(w.jobs) {
		return c.violation(w, "no-job-lost", "store has %d jobs, workload had %d", len(all), len(w.jobs))
	}
	for _, j := range all {
		if j.State != types.Completed {
			return c.violation(w, "no-job-lost", "job %s ended %s (attempt %d)", j.ID, j.State, j.Attempt)
		}
		if c.accepted[j.ID] != 1 {
			return c.violation(w, "no-double-run", "job %s has %d accepted completions", j.ID, c.accepted[j.ID])
		}
	}
	return nil
}
