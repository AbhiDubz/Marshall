package sched

// Test suite for stubs #2 and #3: BackfillScheduler.Schedule and
// BackfillScheduler.computeReservation. These tests FAIL until the
// stubs are implemented; the helpers convert the stubs' panics into
// test failures so the rest of the package's tests still run.

import (
	"reflect"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

func bfSchedule(t *testing.T, s *BackfillScheduler, now time.Time, queue []types.Job,
	nodes []types.Node, running []types.Allocation) []types.Allocation {
	t.Helper()
	var out []types.Allocation
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("BackfillScheduler.Schedule panicked: %v (stub #2 — implement it)", r)
			}
		}()
		out = s.Schedule(now, queue, nodes, running)
	}()
	return out
}

func bfReserve(t *testing.T, s *BackfillScheduler, now time.Time, head types.Job,
	nodes []types.Node, running []types.Allocation) (time.Time, []string, bool) {
	t.Helper()
	var (
		at  time.Time
		ids []string
		ok  bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("BackfillScheduler.computeReservation panicked: %v (stub #3 — implement it)", r)
			}
		}()
		at, ids, ok = s.computeReservation(now, head, nodes, running)
	}()
	return at, ids, ok
}

// lookupFrom builds a JobLookup over a fixed set of jobs.
func lookupFrom(jobs ...types.Job) JobLookup {
	m := make(map[string]types.Job, len(jobs))
	for _, j := range jobs {
		m[j.ID] = j
	}
	return func(id string) (types.Job, bool) {
		j, ok := m[id]
		return j, ok
	}
}

// occupy marks a running job's request as allocated on a node.
func occupy(n types.Node, req types.ResourceSpec) types.Node {
	n.Allocated = n.Allocated.Add(req)
	return n
}

func TestBackfillShortJobFillsGap(t *testing.T) {
	runA := mkJob("runA", 5, 4000, 1, -2*time.Minute, 10*time.Minute)
	runA.State = types.Running
	now := t0 // runA started at t0-2m, est end t0+8m

	nodes := []types.Node{
		occupy(mkNode("n1", 4000, 8), runA.Request), // full with runA
		mkNode("n2", 4000, 8),                       // free
	}
	head := mkJob("head", 9, 3000, 2, -time.Minute, 30*time.Minute) // needs both nodes: blocked
	short := mkJob("short", 1, 1000, 1, 0, 5*time.Minute)           // fits gap, ends t0+5m < t0+8m

	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, head, short))
	running := []types.Allocation{{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-2 * time.Minute)}}

	got := allocIDs(bfSchedule(t, s, now, []types.Job{head, short}, nodes, running))
	if got["head"] != nil {
		t.Fatalf("blocked head must not start: %v", got)
	}
	if !reflect.DeepEqual(got["short"], []string{"n2"}) {
		t.Fatalf("short job projected to finish before the reservation must be backfilled on n2, got %v", got)
	}
}

func TestBackfillDoesNotDelayReservedJob(t *testing.T) {
	runA := mkJob("runA", 5, 4000, 1, -2*time.Minute, 10*time.Minute)
	runA.State = types.Running
	now := t0

	nodes := []types.Node{
		occupy(mkNode("n1", 4000, 8), runA.Request),
		mkNode("n2", 4000, 8),
	}
	head := mkJob("head", 9, 3000, 2, -time.Minute, 30*time.Minute) // blocked; reservation t0+8m on {n1,n2}
	long := mkJob("long", 1, 1000, 1, 0, 20*time.Minute)            // outlives reservation, only fit is reserved n2

	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, head, long))
	running := []types.Allocation{{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-2 * time.Minute)}}

	got := bfSchedule(t, s, now, []types.Job{head, long}, nodes, running)
	if len(got) != 0 {
		t.Fatalf("a job that would delay the reserved head must not be backfilled, got %v", got)
	}
}

func TestBackfillLongJobAvoidingReservedNodesIsBackfilled(t *testing.T) {
	runA := mkJob("runA", 5, 4000, 1, -2*time.Minute, 10*time.Minute)
	runA.State = types.Running
	now := t0

	nodes := []types.Node{
		occupy(mkNode("n1", 4000, 8), runA.Request), // full until t0+8m
		mkNode("n2", 4000, 8),                       // free, big enough for head
		mkNode("n3", 2000, 8),                       // free, too small for head
	}
	// head needs 2 nodes of 3000m: only n1, n2 qualify -> blocked now,
	// reservation t0+8m on {n1,n2}. n3 is never reserved.
	head := mkJob("head", 9, 3000, 2, -time.Minute, 30*time.Minute)
	long := mkJob("long", 1, 1000, 1, 0, 20*time.Minute) // outlives R but fits on unreserved n3

	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, head, long))
	running := []types.Allocation{{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-2 * time.Minute)}}

	got := allocIDs(bfSchedule(t, s, now, []types.Job{head, long}, nodes, running))
	if !reflect.DeepEqual(got["long"], []string{"n3"}) {
		t.Fatalf("long job avoiding all reserved nodes must be backfilled on n3, got %v", got)
	}
}

func TestComputeReservationAccurateEstimates(t *testing.T) {
	runA := mkJob("runA", 5, 4000, 1, -2*time.Minute, 10*time.Minute) // ends t0+8m
	runA.State = types.Running
	runB := mkJob("runB", 5, 4000, 1, -time.Minute, 4*time.Minute) // ends t0+3m
	runB.State = types.Running
	now := t0

	nodes := []types.Node{
		occupy(mkNode("n1", 4000, 8), runA.Request),
		occupy(mkNode("n2", 4000, 8), runB.Request),
	}
	head := mkJob("head", 9, 3000, 2, 0, 30*time.Minute)
	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, runB, head))
	running := []types.Allocation{
		{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-2 * time.Minute)},
		{JobID: "runB", NodeIDs: []string{"n2"}, StartAt: t0.Add(-time.Minute)},
	}

	at, ids, ok := bfReserve(t, s, now, head, nodes, running)
	if !ok {
		t.Fatal("reservation must be satisfiable")
	}
	// Both nodes are needed; the later release (runA at t0+8m) gates it.
	if want := t0.Add(8 * time.Minute); !at.Equal(want) {
		t.Fatalf("reservation at %v, want %v (when both nodes are free)", at, want)
	}
	wantIDs := []string{"n1", "n2"}
	gotIDs := append([]string(nil), ids...)
	if !reflect.DeepEqual(sortedCopy(gotIDs), wantIDs) {
		t.Fatalf("reserved nodes %v, want %v", ids, wantIDs)
	}
}

func TestComputeReservationClampsOverrunToNow(t *testing.T) {
	// runA is 20 minutes past its estimate. Its projected release must
	// clamp to `now`, never a time in the past.
	runA := mkJob("runA", 5, 4000, 1, -30*time.Minute, 10*time.Minute)
	runA.State = types.Running
	now := t0 // est end was t0-20m

	nodes := []types.Node{occupy(mkNode("n1", 4000, 8), runA.Request)}
	head := mkJob("head", 9, 3000, 1, 0, 30*time.Minute)
	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, head))
	running := []types.Allocation{{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-30 * time.Minute)}}

	at, _, ok := bfReserve(t, s, now, head, nodes, running)
	if !ok {
		t.Fatal("reservation must be satisfiable")
	}
	if at.Before(now) {
		t.Fatalf("reservation %v is in the past (now=%v): overrun not clamped", at, now)
	}
	if !at.Equal(now) {
		t.Fatalf("overrun job should project release at now=%v, got %v", now, at)
	}
}

func TestComputeReservationUnknownJobIsInfinite(t *testing.T) {
	// The only node head could use is held by a job the lookup cannot
	// resolve — no reservation is satisfiable.
	mystery := types.Allocation{JobID: "unknown", NodeIDs: []string{"n1"}, StartAt: t0.Add(-time.Minute)}
	nodes := []types.Node{occupy(mkNode("n1", 4000, 8), types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 1 << 30})}
	head := mkJob("head", 9, 3000, 1, 0, 30*time.Minute)
	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(head))

	_, _, ok := bfReserve(t, s, t0, head, nodes, []types.Allocation{mystery})
	if ok {
		t.Fatal("reservation must be unsatisfiable when the blocking job's estimate is unknown")
	}
}

func TestBackfillOverrunDoesNotGrantPhantomResources(t *testing.T) {
	// runA overruns and still holds n1. The reservation clamps to now
	// on {n1,n2}; the short candidate cannot finish "by now" and has
	// no unreserved node to use, so nothing may start — in particular
	// nothing may be placed on capacity runA still physically holds.
	runA := mkJob("runA", 5, 4000, 1, -30*time.Minute, 10*time.Minute)
	runA.State = types.Running
	now := t0

	nodes := []types.Node{
		occupy(mkNode("n1", 4000, 8), runA.Request),
		mkNode("n2", 4000, 8),
	}
	head := mkJob("head", 9, 3000, 2, -time.Minute, 30*time.Minute)
	short := mkJob("short", 1, 1000, 1, 0, time.Second)

	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, head, short))
	running := []types.Allocation{{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-30 * time.Minute)}}

	got := bfSchedule(t, s, now, []types.Job{head, short}, nodes, running)
	for _, a := range got {
		for _, id := range a.NodeIDs {
			if id == "n1" {
				t.Fatalf("allocation %v uses n1, which runA still physically occupies", a)
			}
		}
	}
	if len(got) != 0 {
		t.Fatalf("nothing can start without risking the clamped reservation, got %v", got)
	}
}

func TestBackfillEqualPriorityTieBreaksBySubmitTime(t *testing.T) {
	// One free slot, two equal-priority candidates; the earlier submit
	// wins, regardless of the order the queue slice arrives in.
	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		nodes := []types.Node{mkNode("n1", 4000, 8)}
		early := mkJob("early", 3, 3000, 1, 0, time.Minute)
		late := mkJob("late", 3, 3000, 1, time.Second, time.Minute)
		jobs := [2]types.Job{early, late}
		queue := []types.Job{jobs[order[0]], jobs[order[1]]}

		s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(early, late))
		got := allocIDs(bfSchedule(t, s, t0.Add(2*time.Second), queue, nodes, nil))
		if got["early"] == nil || got["late"] != nil {
			t.Fatalf("order %v: tie must break by submit time (early wins), got %v", order, got)
		}
	}
}

func TestBackfillAllFitBehavesLikeFIFO(t *testing.T) {
	nodes := []types.Node{mkNode("n1", 4000, 8), mkNode("n2", 4000, 8)}
	a := mkJob("a", 2, 2000, 1, 0, time.Minute)
	b := mkJob("b", 1, 2000, 1, time.Second, time.Minute)
	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(a, b))
	got := allocIDs(bfSchedule(t, s, t0.Add(time.Minute), []types.Job{a, b}, nodes, nil))
	if got["a"] == nil || got["b"] == nil {
		t.Fatalf("with no blocker every fitting job starts, got %v", got)
	}
}

func TestBackfillDoesNotMutateInputs(t *testing.T) {
	runA := mkJob("runA", 5, 4000, 1, -2*time.Minute, 10*time.Minute)
	runA.State = types.Running
	nodes := []types.Node{occupy(mkNode("n1", 4000, 8), runA.Request), mkNode("n2", 4000, 8)}
	head := mkJob("head", 9, 3000, 2, -time.Minute, 30*time.Minute)
	short := mkJob("short", 1, 1000, 1, 0, 5*time.Minute)
	queue := []types.Job{head, short}
	running := []types.Allocation{{JobID: "runA", NodeIDs: []string{"n1"}, StartAt: t0.Add(-2 * time.Minute)}}

	nodesSnap := []types.Node{nodes[0].Clone(), nodes[1].Clone()}
	queueSnap := []types.Job{queue[0].Clone(), queue[1].Clone()}
	runSnap := []types.Allocation{running[0].Clone()}

	s := NewBackfillScheduler(alloc.FirstFitAllocator{}, lookupFrom(runA, head, short))
	bfSchedule(t, s, t0, queue, nodes, running)

	for i := range nodes {
		if !reflect.DeepEqual(nodes[i], nodesSnap[i]) {
			t.Fatalf("Schedule mutated nodes[%d]: %+v", i, nodes[i])
		}
	}
	for i := range queue {
		if !reflect.DeepEqual(queue[i], queueSnap[i]) {
			t.Fatalf("Schedule mutated queue[%d]: %+v", i, queue[i])
		}
	}
	if !reflect.DeepEqual(running[0], runSnap[0]) {
		t.Fatalf("Schedule mutated running[0]: %+v", running[0])
	}
}

func sortedCopy(ids []string) []string {
	out := append([]string(nil), ids...)
	for i := 1; i < len(out); i++ {
		for k := i; k > 0 && out[k] < out[k-1]; k-- {
			out[k], out[k-1] = out[k-1], out[k]
		}
	}
	return out
}
