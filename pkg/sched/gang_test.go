package sched

// Test suite for GangScheduler.accumulate (formerly stub #4). The
// helpers keep converting any panic into a clean test failure so the
// rest of the package's tests still run.

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

func gsAccumulate(t *testing.T, s *GangScheduler, now time.Time, job types.Job, work []types.Node) ([]string, bool) {
	t.Helper()
	var (
		ids   []string
		ready bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("GangScheduler.accumulate panicked: %v (stub #4 — implement it)", r)
			}
		}()
		ids, ready = s.accumulate(now, job, work)
	}()
	return ids, ready
}

func gsSchedule(t *testing.T, s Scheduler, now time.Time, queue []types.Job,
	nodes []types.Node, running []types.Allocation) []types.Allocation {
	t.Helper()
	var out []types.Allocation
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Schedule panicked: %v (stub #4 GangScheduler.accumulate — implement it)", r)
			}
		}()
		out = s.Schedule(now, queue, nodes, running)
	}()
	return out
}

func TestAccumulateClaimsFittingNodesInIDOrder(t *testing.T) {
	s := NewGangScheduler(alloc.FirstFitAllocator{})
	gang := mkJob("gang", 5, 2000, 3, 0, time.Minute)
	work := []types.Node{mkNode("n2", 4000, 8), mkNode("n1", 4000, 8)} // only two fit yet

	ids, ready := gsAccumulate(t, s, t0, gang, work)
	if ready {
		t.Fatalf("only 2 of 3 nodes available, must not be ready: %v", ids)
	}
	if !reflect.DeepEqual(sortedCopy(ids), []string{"n1", "n2"}) {
		t.Fatalf("want both fitting nodes claimed (ID order), got %v", ids)
	}
}

func TestAccumulateIsSticky(t *testing.T) {
	// The anti-starvation ratchet: a node claimed in cycle k stays
	// claimed in cycle k+1 even if no new node appeared.
	s := NewGangScheduler(alloc.FirstFitAllocator{})
	gang := mkJob("gang", 5, 2000, 4, 0, time.Minute)
	work := []types.Node{mkNode("n1", 4000, 8), mkNode("n2", 4000, 8)}

	first, _ := gsAccumulate(t, s, t0, gang, work)
	second, _ := gsAccumulate(t, s, t0.Add(time.Second), gang, work)
	if !reflect.DeepEqual(sortedCopy(first), sortedCopy(second)) {
		t.Fatalf("hold changed with no cluster change: %v -> %v", first, second)
	}

	// A third node appears; the hold grows, never shrinks.
	work = append(work, mkNode("n3", 4000, 8))
	third, ready := gsAccumulate(t, s, t0.Add(2*time.Second), gang, work)
	if ready {
		t.Fatalf("3 of 4 held, must not be ready: %v", third)
	}
	if !reflect.DeepEqual(sortedCopy(third), []string{"n1", "n2", "n3"}) {
		t.Fatalf("hold must grow monotonically, got %v", third)
	}
}

func TestAccumulateReadyExactlyAtNodeCount(t *testing.T) {
	s := NewGangScheduler(alloc.FirstFitAllocator{})
	gang := mkJob("gang", 5, 2000, 2, 0, time.Minute)
	work := []types.Node{mkNode("n1", 4000, 8), mkNode("n2", 4000, 8), mkNode("n3", 4000, 8)}

	ids, ready := gsAccumulate(t, s, t0, gang, work)
	if !ready {
		t.Fatalf("2 fitting nodes available for NodeCount=2, must be ready: %v", ids)
	}
	if len(ids) != 2 {
		t.Fatalf("hold must contain exactly NodeCount nodes, got %v", ids)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate node in hold: %v", ids)
		}
		seen[id] = true
	}
}

func TestAccumulateDropsInvalidNodes(t *testing.T) {
	s := NewGangScheduler(alloc.FirstFitAllocator{})
	gang := mkJob("gang", 5, 2000, 3, 0, time.Minute)
	work := []types.Node{mkNode("n1", 4000, 8), mkNode("n2", 4000, 8)}
	gsAccumulate(t, s, t0, gang, work)

	// n1 starts draining; it must fall out of the hold.
	work[0].Draining = true
	ids, ready := gsAccumulate(t, s, t0.Add(time.Second), gang, work)
	if ready {
		t.Fatalf("must not be ready, got %v", ids)
	}
	for _, id := range ids {
		if id == "n1" {
			t.Fatalf("draining node must be dropped from hold: %v", ids)
		}
	}

	// n2 disappears entirely (died); hold must not reference it.
	ids, _ = gsAccumulate(t, s, t0.Add(2*time.Second), gang, []types.Node{{ID: "n3", Capacity: types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30}}})
	for _, id := range ids {
		if id == "n1" || id == "n2" {
			t.Fatalf("dead node must be dropped from hold: %v", ids)
		}
	}
}

func TestAccumulateSkipsNodesThatCannotFit(t *testing.T) {
	s := NewGangScheduler(alloc.FirstFitAllocator{})
	gang := mkJob("gang", 5, 3000, 2, 0, time.Minute)
	full := mkNode("n1", 4000, 8)
	full.Allocated = types.ResourceSpec{CPUMillis: 2000, MemoryBytes: 1 << 30} // only 2000m free
	work := []types.Node{full, mkNode("n2", 4000, 8)}

	ids, _ := gsAccumulate(t, s, t0, gang, work)
	for _, id := range ids {
		if id == "n1" {
			t.Fatalf("node without room for the request must not be held: %v", ids)
		}
	}
}

func TestGangScheduleHoldsNodesAgainstSmallJobs(t *testing.T) {
	// One free node, gang job needs two. The free node must be held
	// for the gang job, NOT handed to the small job behind it.
	s := NewGangScheduler(alloc.FirstFitAllocator{})
	busy := mkNode("n1", 4000, 8)
	busy.Allocated = types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30}
	nodes := []types.Node{busy, mkNode("n2", 4000, 8)}
	gang := mkJob("gang", 5, 2000, 2, 0, time.Hour)
	small := mkJob("small", 5, 2000, 1, time.Second, time.Minute)

	got := gsSchedule(t, s, t0.Add(2*time.Second), []types.Job{gang, small}, nodes, nil)
	for _, a := range got {
		if a.JobID == "small" {
			t.Fatalf("free node must be held for the gang job, not given to small: %v", got)
		}
	}
}

// tickHarness is a miniature deterministic cluster loop used by the
// starvation tests: 4 nodes, sustained single-node load, one 4-node
// gang job. It returns the gang job's start delay and true, or false
// if it never started within the horizon.
func tickHarness(t *testing.T, s Scheduler) (time.Duration, bool) {
	t.Helper()
	const (
		nodeCount    = 4
		horizon      = 300 // seconds
		smallEvery   = 2   // a new small job every 2s: sustained load
		smallRuntime = 10 * time.Second
		gangSubmit   = 1 // second
	)
	nodes := make([]types.Node, nodeCount)
	for i := range nodes {
		nodes[i] = mkNode(fmt.Sprintf("n%d", i+1), 4000, 8)
	}
	type runrec struct {
		alloc types.Allocation
		req   types.ResourceSpec
		end   time.Time
	}
	running := map[string]*runrec{}
	var pending []types.Job
	gangStart := time.Time{}
	gangSubmitted := t0.Add(gangSubmit * time.Second)

	for sec := 0; sec <= horizon; sec++ {
		now := t0.Add(time.Duration(sec) * time.Second)

		for id, r := range running {
			if !r.end.After(now) {
				for _, nid := range r.alloc.NodeIDs {
					for i := range nodes {
						if nodes[i].ID == nid {
							nodes[i].Allocated = nodes[i].Allocated.Sub(r.req)
						}
					}
				}
				delete(running, id)
			}
		}

		if sec%smallEvery == 0 {
			pending = append(pending, mkJob(fmt.Sprintf("small-%03d", sec), 5, 4000, 1,
				time.Duration(sec)*time.Second, smallRuntime))
		}
		if sec == gangSubmit {
			pending = append(pending, mkJob("gang", 5, 4000, nodeCount,
				time.Duration(sec)*time.Second, 20*time.Second))
		}

		var runList []types.Allocation
		for _, r := range running {
			runList = append(runList, r.alloc)
		}
		allocs := gsSchedule(t, s, now, pending, nodes, runList)

		for _, a := range allocs {
			var job types.Job
			idx := -1
			for i, p := range pending {
				if p.ID == a.JobID {
					job, idx = p, i
					break
				}
			}
			if idx == -1 {
				t.Fatalf("scheduler returned unknown job %s", a.JobID)
			}
			for _, nid := range a.NodeIDs {
				for i := range nodes {
					if nodes[i].ID == nid {
						if !job.Request.Fits(nodes[i].Available()) {
							t.Fatalf("double-booked node %s for job %s", nid, job.ID)
						}
						nodes[i].Allocated = nodes[i].Allocated.Add(job.Request)
					}
				}
			}
			runtime := smallRuntime
			if job.ID == "gang" {
				runtime = 20 * time.Second
				gangStart = now
			}
			running[job.ID] = &runrec{alloc: a, req: job.Request, end: now.Add(runtime)}
			pending = append(pending[:idx], pending[idx+1:]...)
		}
		if !gangStart.IsZero() {
			return gangStart.Sub(gangSubmitted), true
		}
	}
	return 0, false
}

// TestGangStarvationRegression is the explicit starvation regression
// test the brief requires: under sustained small-job load, the 4-node
// gang job must start within a bounded wait. Any non-reserving
// strategy fails this (see TestNaiveGreedyStarvesGangJob, which proves
// the harness has teeth).
func TestGangStarvationRegression(t *testing.T) {
	const bound = 60 * time.Second
	wait, started := tickHarness(t, NewGangScheduler(alloc.FirstFitAllocator{}))
	if !started {
		t.Fatal("gang job never started within the horizon: starvation")
	}
	if wait > bound {
		t.Fatalf("gang job waited %v, bound is %v", wait, bound)
	}
}

// naiveGreedy schedules anything that fits, in canonical order, with
// no reservations — the strawman that starves gang jobs.
type naiveGreedy struct{ a Allocator }

func (g naiveGreedy) Schedule(now time.Time, queue []types.Job, nodes []types.Node,
	running []types.Allocation) []types.Allocation {
	work := cloneNodes(nodes)
	q := cloneQueue(queue)
	var out []types.Allocation
	for _, job := range q {
		if job.State != types.Pending {
			continue
		}
		nc := job.NodeCount
		if nc == 0 {
			nc = 1
		}
		if ids, ok := g.a.Fit(work, job.Request, nc); ok {
			charge(work, ids, job.Request)
			out = append(out, types.Allocation{JobID: job.ID, NodeIDs: ids, StartAt: now})
		}
	}
	return out
}
func (naiveGreedy) Name() string { return "naive-greedy" }

// TestNaiveGreedyStarvesGangJob documents that the starvation harness
// genuinely discriminates: a non-reserving scheduler never starts the
// gang job. If this test ever fails, the harness has lost its teeth
// and TestGangStarvationRegression proves nothing.
func TestNaiveGreedyStarvesGangJob(t *testing.T) {
	wait, started := tickHarness(t, naiveGreedy{a: alloc.FirstFitAllocator{}})
	if started && wait <= 60*time.Second {
		t.Fatalf("naive greedy scheduler started the gang job in %v — the starvation harness no longer discriminates", wait)
	}
}
