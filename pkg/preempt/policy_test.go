package preempt

// Test suite for stub #5: Policy.SelectVictims. These tests FAIL until
// the stub is implemented; svSelect converts the stub's panic into a
// test failure. Requeue and fair-share tests pass already.

import (
	"reflect"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func svSelect(t *testing.T, p *Policy, pending types.Job, running []Candidate, nodes []types.Node) ([]string, bool) {
	t.Helper()
	var (
		victims []string
		ok      bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Policy.SelectVictims panicked: %v (stub #5 — implement it)", r)
			}
		}()
		victims, ok = p.SelectVictims(pending, running, nodes)
	}()
	return victims, ok
}

func node(id string, cpu int64) types.Node {
	return types.Node{ID: id, Capacity: types.ResourceSpec{CPUMillis: cpu, MemoryBytes: 16 << 30}}
}

func pjob(id string, prio int, cpu int64, nodeCount int, startedAgo time.Duration) types.Job {
	return types.Job{
		ID: id, User: "u", Priority: prio,
		Request:   types.ResourceSpec{CPUMillis: cpu, MemoryBytes: 1 << 30},
		NodeCount: nodeCount,
		SubmitAt:  t0.Add(-startedAgo - time.Minute),
		State:     types.Running,
	}
}

// place wires a running candidate onto nodes, charging Allocated.
func place(nodes []types.Node, j types.Job, startedAgo time.Duration, nodeIDs ...string) Candidate {
	for _, id := range nodeIDs {
		for i := range nodes {
			if nodes[i].ID == id {
				nodes[i].Allocated = nodes[i].Allocated.Add(j.Request)
			}
		}
	}
	return Candidate{Job: j, Alloc: types.Allocation{JobID: j.ID, NodeIDs: nodeIDs, StartAt: t0.Add(-startedAgo)}}
}

func TestSelectVictimsMinimalSet(t *testing.T) {
	// Evicting one 4000m job (low2) beats evicting two 2000m jobs.
	nodes := []types.Node{node("n1", 4000), node("n2", 4000)}
	running := []Candidate{
		place(nodes, pjob("low1a", 1, 2000, 1, 10*time.Minute), 10*time.Minute, "n1"),
		place(nodes, pjob("low1b", 1, 2000, 1, 10*time.Minute), 10*time.Minute, "n1"),
		place(nodes, pjob("low2", 1, 4000, 1, 10*time.Minute), 10*time.Minute, "n2"),
	}
	pending := pjob("high", 9, 4000, 1, 0)
	pending.State = types.Pending

	victims, ok := svSelect(t, NewPolicy(alloc.FirstFitAllocator{}), pending, running, nodes)
	if !ok {
		t.Fatal("preemption must be possible")
	}
	if !reflect.DeepEqual(victims, []string{"low2"}) {
		t.Fatalf("minimal victim set is [low2] (one eviction), got %v", victims)
	}
}

func TestSelectVictimsNeverEqualOrHigherPriority(t *testing.T) {
	nodes := []types.Node{node("n1", 4000), node("n2", 4000)}
	running := []Candidate{
		place(nodes, pjob("same", 5, 4000, 1, time.Minute), time.Minute, "n1"),
		place(nodes, pjob("higher", 9, 4000, 1, time.Minute), time.Minute, "n2"),
	}
	pending := pjob("p", 5, 4000, 1, 0)
	pending.State = types.Pending

	victims, ok := svSelect(t, NewPolicy(alloc.FirstFitAllocator{}), pending, running, nodes)
	if ok || victims != nil {
		t.Fatalf("equal/higher priority jobs must never be preempted, got %v %v", victims, ok)
	}
}

func TestSelectVictimsNoneNeededWhenFits(t *testing.T) {
	nodes := []types.Node{node("n1", 4000), node("n2", 4000)}
	running := []Candidate{
		place(nodes, pjob("low", 1, 4000, 1, time.Minute), time.Minute, "n1"),
	}
	pending := pjob("p", 9, 2000, 1, 0)
	pending.State = types.Pending

	victims, ok := svSelect(t, NewPolicy(alloc.FirstFitAllocator{}), pending, running, nodes)
	if !ok || len(victims) != 0 {
		t.Fatalf("pending fits on n2 without preemption; want ([], true), got (%v, %v)", victims, ok)
	}
}

func TestSelectVictimsMultiNodePending(t *testing.T) {
	nodes := []types.Node{node("n1", 4000), node("n2", 4000), node("n3", 4000)}
	running := []Candidate{
		place(nodes, pjob("v1", 1, 3000, 1, time.Minute), time.Minute, "n1"),
		place(nodes, pjob("v2", 2, 3000, 1, time.Minute), time.Minute, "n2"),
	}
	pending := pjob("gangy", 9, 3000, 2, 0)
	pending.State = types.Pending

	victims, ok := svSelect(t, NewPolicy(alloc.FirstFitAllocator{}), pending, running, nodes)
	if !ok {
		t.Fatal("preemption must be possible")
	}
	// n3 is free; one eviction opens the second node. The cheaper
	// victim by priority is v1.
	if !reflect.DeepEqual(victims, []string{"v1"}) {
		t.Fatalf("want [v1] (free n3 + cheapest single eviction), got %v", victims)
	}
}

func TestSelectVictimsGangVictimFreesAllItsNodes(t *testing.T) {
	nodes := []types.Node{node("n1", 4000), node("n2", 4000), node("n3", 4000), node("n4", 4000)}
	g := pjob("gangvictim", 1, 4000, 2, time.Minute)
	running := []Candidate{
		place(nodes, g, time.Minute, "n1", "n2"),
		place(nodes, pjob("s1", 1, 4000, 1, time.Minute), time.Minute, "n3"),
		place(nodes, pjob("s2", 1, 4000, 1, time.Minute), time.Minute, "n4"),
	}
	pending := pjob("big", 9, 4000, 2, 0)
	pending.State = types.Pending

	victims, ok := svSelect(t, NewPolicy(alloc.FirstFitAllocator{}), pending, running, nodes)
	if !ok {
		t.Fatal("preemption must be possible")
	}
	if !reflect.DeepEqual(victims, []string{"gangvictim"}) {
		t.Fatalf("one gang victim frees both nodes — minimal set is [gangvictim], got %v", victims)
	}
}

func TestSelectVictimsImpossible(t *testing.T) {
	nodes := []types.Node{node("n1", 4000), node("n2", 4000)}
	running := []Candidate{
		place(nodes, pjob("hi1", 9, 4000, 1, time.Minute), time.Minute, "n1"),
		place(nodes, pjob("low", 1, 4000, 1, time.Minute), time.Minute, "n2"),
	}
	// Needs 2 nodes but only one is occupied by preemptible work.
	pending := pjob("p", 5, 4000, 2, 0)
	pending.State = types.Pending

	victims, ok := svSelect(t, NewPolicy(alloc.FirstFitAllocator{}), pending, running, nodes)
	if ok || victims != nil {
		t.Fatalf("placement impossible even with preemption; want (nil,false), got (%v,%v)", victims, ok)
	}
}

func TestSelectVictimsNoCascade(t *testing.T) {
	// After the high-priority job takes low2's node, the requeued
	// victim must not be able to preempt anybody.
	nodes := []types.Node{node("n1", 4000), node("n2", 4000)}
	running := []Candidate{
		place(nodes, pjob("low1a", 1, 2000, 1, 10*time.Minute), 10*time.Minute, "n1"),
		place(nodes, pjob("low1b", 1, 2000, 1, 10*time.Minute), 10*time.Minute, "n1"),
		place(nodes, pjob("low2", 1, 4000, 1, 10*time.Minute), 10*time.Minute, "n2"),
	}
	pending := pjob("high", 9, 4000, 1, 0)
	pending.State = types.Pending

	p := NewPolicy(alloc.FirstFitAllocator{})
	victims, ok := svSelect(t, p, pending, running, nodes)
	if !ok || len(victims) != 1 || victims[0] != "low2" {
		t.Fatalf("setup: want [low2], got (%v,%v)", victims, ok)
	}

	// Apply: low2 evicted + requeued; high runs on n2.
	afterNodes := []types.Node{node("n1", 4000), node("n2", 4000)}
	afterRunning := []Candidate{
		place(afterNodes, pjob("low1a", 1, 2000, 1, 10*time.Minute), 10*time.Minute, "n1"),
		place(afterNodes, pjob("low1b", 1, 2000, 1, 10*time.Minute), 10*time.Minute, "n1"),
		place(afterNodes, pjob("high", 9, 4000, 1, 0), 0, "n2"),
	}
	evicted := pjob("low2", 1, 4000, 1, 0)
	evicted.State = types.Preempted
	requeued, err := Requeue(evicted)
	if err != nil {
		t.Fatal(err)
	}
	if v2, ok2 := svSelect(t, p, requeued, afterRunning, afterNodes); ok2 || v2 != nil {
		t.Fatalf("preemption cascade: requeued victim evicted (%v,%v)", v2, ok2)
	}
}

func TestSelectVictimsDeterministicUnderShuffle(t *testing.T) {
	build := func(nodeOrder []string, candOrder []int) ([]types.Node, []Candidate) {
		all := map[string]types.Node{
			"n1": node("n1", 4000), "n2": node("n2", 4000), "n3": node("n3", 4000),
		}
		var nodes []types.Node
		for _, id := range nodeOrder {
			nodes = append(nodes, all[id])
		}
		jobs := []struct {
			id   string
			prio int
			node string
		}{{"a", 1, "n1"}, {"b", 2, "n2"}, {"c", 1, "n3"}}
		var cands []Candidate
		for _, i := range candOrder {
			j := jobs[i]
			cands = append(cands, place(nodes, pjob(j.id, j.prio, 4000, 1, time.Minute), time.Minute, j.node))
		}
		return nodes, cands
	}
	pending := pjob("p", 9, 4000, 1, 0)
	pending.State = types.Pending
	p := NewPolicy(alloc.FirstFitAllocator{})

	var want []string
	first := true
	for _, no := range [][]string{{"n1", "n2", "n3"}, {"n3", "n1", "n2"}} {
		for _, co := range [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}} {
			nodes, cands := build(no, co)
			victims, ok := svSelect(t, p, pending, cands, nodes)
			if !ok {
				t.Fatal("must be possible")
			}
			if first {
				want, first = victims, false
			} else if !reflect.DeepEqual(victims, want) {
				t.Fatalf("order sensitivity: %v vs %v (nodes %v cands %v)", victims, want, no, co)
			}
		}
	}
}

func TestRequeueIncrementsAttempt(t *testing.T) {
	j := pjob("v", 1, 1000, 1, time.Minute)
	j.State = types.Preempted
	j.Attempt = 2
	out, err := Requeue(j)
	if err != nil {
		t.Fatal(err)
	}
	if out.State != types.Pending || out.Attempt != 3 {
		t.Fatalf("want PENDING attempt 3, got %s attempt %d", out.State, out.Attempt)
	}
	// Original untouched; requeue only from PREEMPTED.
	if j.Attempt != 2 || j.State != types.Preempted {
		t.Fatalf("Requeue mutated its input: %+v", j)
	}
	j.State = types.Running
	if _, err := Requeue(j); err == nil {
		t.Fatal("requeue from RUNNING must error")
	}
}
