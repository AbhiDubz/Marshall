package sched

import (
	"reflect"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func mkNode(id string, cpu, memGB int64) types.Node {
	return types.Node{
		ID:       id,
		Capacity: types.ResourceSpec{CPUMillis: cpu, MemoryBytes: memGB << 30},
	}
}

func mkJob(id string, prio int, cpu int64, nodeCount int, submitOffset, est time.Duration) types.Job {
	return types.Job{
		ID: id, User: "u", Priority: prio,
		Request:    types.ResourceSpec{CPUMillis: cpu, MemoryBytes: 1 << 30},
		NodeCount:  nodeCount,
		EstRuntime: est,
		SubmitAt:   t0.Add(submitOffset),
		State:      types.Pending,
	}
}

func allocIDs(allocs []types.Allocation) map[string][]string {
	out := map[string][]string{}
	for _, a := range allocs {
		out[a.JobID] = a.NodeIDs
	}
	return out
}

func TestFIFOStartsInPriorityThenSubmitOrder(t *testing.T) {
	s := NewFIFOScheduler(alloc.FirstFitAllocator{})
	nodes := []types.Node{mkNode("n1", 4000, 8), mkNode("n2", 4000, 8)}
	queue := []types.Job{
		mkJob("low", 1, 2000, 1, 0, time.Minute),
		mkJob("high", 5, 2000, 1, time.Second, time.Minute),
	}
	got := s.Schedule(t0.Add(2*time.Second), queue, nodes, nil)
	if len(got) != 2 {
		t.Fatalf("want 2 allocations, got %v", got)
	}
	if got[0].JobID != "high" || got[1].JobID != "low" {
		t.Fatalf("priority order violated: %v", got)
	}
	for _, a := range got {
		if !a.StartAt.Equal(t0.Add(2 * time.Second)) {
			t.Fatalf("StartAt must be `now`, got %v", a.StartAt)
		}
	}
}

func TestFIFOHeadOfLineBlocks(t *testing.T) {
	// Head job needs 2 nodes; only 1 is free. The small job behind it
	// would fit, but FIFO never skips.
	s := NewFIFOScheduler(alloc.FirstFitAllocator{})
	n1 := mkNode("n1", 4000, 8)
	n1.Allocated = types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30}
	nodes := []types.Node{n1, mkNode("n2", 4000, 8)}
	queue := []types.Job{
		mkJob("bigGang", 5, 1000, 2, 0, time.Minute),
		mkJob("small", 1, 100, 1, time.Second, time.Second),
	}
	got := s.Schedule(t0.Add(2*time.Second), queue, nodes, nil)
	if len(got) != 0 {
		t.Fatalf("strict FIFO must not skip the blocked head, got %v", got)
	}
}

func TestFIFOChargesNodesWithinCycle(t *testing.T) {
	// Two jobs each needing 3000m; one 4000m node. Only the first may
	// start — the working copy must be charged, not the input reread.
	s := NewFIFOScheduler(alloc.FirstFitAllocator{})
	nodes := []types.Node{mkNode("n1", 4000, 8)}
	queue := []types.Job{
		mkJob("a", 1, 3000, 1, 0, time.Minute),
		mkJob("b", 1, 3000, 1, time.Second, time.Minute),
	}
	got := s.Schedule(t0.Add(time.Minute), queue, nodes, nil)
	if len(got) != 1 || got[0].JobID != "a" {
		t.Fatalf("want only job a, got %v", got)
	}
}

func TestFIFODoesNotMutateInputs(t *testing.T) {
	s := NewFIFOScheduler(alloc.FirstFitAllocator{})
	nodes := []types.Node{mkNode("n1", 4000, 8)}
	queue := []types.Job{mkJob("a", 1, 1000, 1, 0, time.Minute)}
	nodesSnap := []types.Node{nodes[0].Clone()}
	queueSnap := []types.Job{queue[0].Clone()}
	s.Schedule(t0.Add(time.Minute), queue, nodes, nil)
	if !reflect.DeepEqual(nodes[0], nodesSnap[0]) {
		t.Fatalf("Schedule mutated nodes: %+v", nodes[0])
	}
	if !reflect.DeepEqual(queue[0], queueSnap[0]) {
		t.Fatalf("Schedule mutated queue: %+v", queue[0])
	}
}

func TestFIFOGangJobAllOrNothing(t *testing.T) {
	s := NewFIFOScheduler(alloc.FirstFitAllocator{})
	nodes := []types.Node{mkNode("n1", 4000, 8), mkNode("n2", 4000, 8), mkNode("n3", 4000, 8)}
	queue := []types.Job{mkJob("gang", 5, 2000, 3, 0, time.Minute)}
	got := s.Schedule(t0.Add(time.Minute), queue, nodes, nil)
	if len(got) != 1 {
		t.Fatalf("want 1 allocation, got %v", got)
	}
	if len(got[0].NodeIDs) != 3 {
		t.Fatalf("gang job must get all 3 nodes at once, got %v", got[0].NodeIDs)
	}
}

func TestFIFOSkipsNonPendingJobs(t *testing.T) {
	s := NewFIFOScheduler(alloc.FirstFitAllocator{})
	nodes := []types.Node{mkNode("n1", 4000, 8)}
	j := mkJob("done", 9, 100, 1, 0, time.Minute)
	j.State = types.Completed
	got := s.Schedule(t0.Add(time.Minute), []types.Job{j, mkJob("p", 1, 100, 1, 0, time.Minute)}, nodes, nil)
	if got := allocIDs(got); got["done"] != nil {
		t.Fatalf("scheduled a non-pending job: %v", got)
	}
}
