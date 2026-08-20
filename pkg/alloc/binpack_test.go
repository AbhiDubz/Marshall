package alloc

// Test suite for BinPackAllocator.Fit (formerly stub #1). bpFit keeps
// converting any panic into a clean test failure instead of crashing
// the package's test binary.

import (
	"reflect"
	"testing"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// bpFit calls BinPackAllocator.Fit, failing (not crashing) the test if
// the stub is unimplemented.
func bpFit(t *testing.T, nodes []types.Node, req types.ResourceSpec, nodeCount int) (ids []string, ok bool) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BinPackAllocator.Fit panicked: %v (stub #1 — implement it)", r)
		}
	}()
	ids, ok = BinPackAllocator{}.Fit(nodes, req, nodeCount)
	return ids, ok
}

func TestBinPackExactFitPreferredOverLooseFit(t *testing.T) {
	// n1 sorts first and fits loosely; n3 fits *exactly*. An exact fit
	// creates zero fragments and must win regardless of ID order.
	nodes := []types.Node{
		mkNode("n1", 8000, 16, nil),
		withAlloc(mkNode("n2", 8000, 16, nil), 5000, 10), // 3000m/6G free: loose
		withAlloc(mkNode("n3", 8000, 16, nil), 6000, 14), // 2000m/2G free: exact
	}
	ids, ok := bpFit(t, nodes, types.ResourceSpec{CPUMillis: 2000, MemoryBytes: 2 << 30}, 1)
	if !ok || ids[0] != "n3" {
		t.Fatalf("got %v %v, want [n3]: exact fit beats loose fit", ids, ok)
	}
}

func TestBinPackPrefersFullerNodes(t *testing.T) {
	// No exact fit anywhere; binpack should pack onto the busier node
	// and keep the empty one whole.
	nodes := []types.Node{
		mkNode("n1", 8000, 16, nil),                     // empty
		withAlloc(mkNode("n2", 8000, 16, nil), 4000, 8), // half full
	}
	ids, ok := bpFit(t, nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: pack busy nodes first, keep empty nodes whole", ids, ok)
	}
}

func TestBinPackFragmentationAvoidanceAcrossSequence(t *testing.T) {
	// Sequence: four 2000m jobs into three 4000m nodes, then one 4000m
	// job. Packing pairs the small jobs onto two nodes and leaves the
	// third empty, so the large job fits. Spreading strands it.
	nodes := []types.Node{
		mkNode("n1", 4000, 8, nil),
		mkNode("n2", 4000, 8, nil),
		mkNode("n3", 4000, 8, nil),
	}
	small := types.ResourceSpec{CPUMillis: 2000, MemoryBytes: 2 << 30}
	place := func(req types.ResourceSpec) string {
		t.Helper()
		ids, ok := bpFit(t, nodes, req, 1)
		if !ok {
			t.Fatalf("placement failed for %+v with nodes %+v", req, nodes)
		}
		for i := range nodes {
			if nodes[i].ID == ids[0] {
				nodes[i].Allocated = nodes[i].Allocated.Add(req)
			}
		}
		return ids[0]
	}
	for i := 0; i < 4; i++ {
		place(small)
	}
	emptyNodes := 0
	for _, n := range nodes {
		if n.Allocated.IsZero() {
			emptyNodes++
		}
	}
	if emptyNodes != 1 {
		t.Fatalf("after four small placements want exactly 1 empty node, got %d (nodes: %+v)", emptyNodes, nodes)
	}
	big := types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 4 << 30}
	if _, ok := bpFit(t, nodes, big, 1); !ok {
		t.Fatal("large job must still fit: packing preserved a whole empty node")
	}
}

func TestBinPackGRESRespected(t *testing.T) {
	nodes := []types.Node{
		mkNode("n1", 8000, 16, map[string]int{"gpu": 2}),
		mkNode("n2", 8000, 16, nil),
	}
	// GPU job can only go to n1.
	ids, ok := bpFit(t, nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30, GRES: map[string]int{"gpu": 2}}, 1)
	if !ok || ids[0] != "n1" {
		t.Fatalf("got %v %v, want [n1]: only GPU node fits", ids, ok)
	}
	// Requesting 3 GPUs when only 2 exist must fail.
	if ids, ok := bpFit(t, nodes, types.ResourceSpec{CPUMillis: 100, GRES: map[string]int{"gpu": 3}}, 1); ok {
		t.Fatalf("GRES over-commit: got %v", ids)
	}
}

func TestBinPackConservesGRESNodes(t *testing.T) {
	// A CPU-only job must prefer the plain node over an otherwise-equal
	// GPU node: burning CPU on the GPU node strands the GPUs.
	nodes := []types.Node{
		mkNode("n1", 8000, 16, map[string]int{"gpu": 4}),
		mkNode("n2", 8000, 16, nil),
	}
	ids, ok := bpFit(t, nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: CPU-only work avoids GPU nodes", ids, ok)
	}
}

func TestBinPackMultiNode(t *testing.T) {
	nodes := []types.Node{
		mkNode("n1", 4000, 8, nil),
		mkNode("n2", 4000, 8, nil),
		mkNode("n3", 4000, 8, nil),
	}
	req := types.ResourceSpec{CPUMillis: 3000, MemoryBytes: 3 << 30}
	ids, ok := bpFit(t, nodes, req, 3)
	if !ok || len(ids) != 3 {
		t.Fatalf("got %v %v, want 3 distinct nodes", ids, ok)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("node %s chosen twice: %v", id, ids)
		}
		seen[id] = true
	}
	// 4 slots into 3 nodes must fail even though each node fits one.
	if ids, ok := bpFit(t, nodes, req, 4); ok {
		t.Fatalf("expected no fit for 4 slots on 3 nodes, got %v", ids)
	}
}

func TestBinPackMultiNodeChargesEarlierSlots(t *testing.T) {
	// n1 has room for exactly one 3000m slot; a 2-slot request must
	// place the second slot elsewhere, not double-book n1.
	nodes := []types.Node{
		withAlloc(mkNode("n1", 8000, 16, nil), 5000, 10),
		mkNode("n2", 8000, 16, nil),
	}
	req := types.ResourceSpec{CPUMillis: 3000, MemoryBytes: 3 << 30}
	ids, ok := bpFit(t, nodes, req, 2)
	if !ok {
		t.Fatal("expected fit")
	}
	if ids[0] == ids[1] {
		t.Fatalf("second slot double-booked node %s", ids[0])
	}
}

func TestBinPackDeterministicUnderShuffle(t *testing.T) {
	base := []types.Node{
		withAlloc(mkNode("n1", 8000, 16, nil), 2000, 4),
		withAlloc(mkNode("n2", 8000, 16, nil), 2000, 4),
		withAlloc(mkNode("n3", 8000, 16, nil), 6000, 12),
		mkNode("n4", 8000, 16, nil),
	}
	perms := [][]int{{0, 1, 2, 3}, {3, 2, 1, 0}, {2, 0, 3, 1}, {1, 3, 0, 2}}
	var want []string
	for i, p := range perms {
		nodes := make([]types.Node, len(base))
		for k, idx := range p {
			nodes[k] = base[idx].Clone()
		}
		ids, ok := bpFit(t, nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 2)
		if !ok {
			t.Fatal("expected fit")
		}
		if i == 0 {
			want = ids
		} else if !reflect.DeepEqual(ids, want) {
			t.Fatalf("permutation %v changed result: %v vs %v", p, ids, want)
		}
	}
}

func TestBinPackDoesNotMutateInput(t *testing.T) {
	nodes := []types.Node{
		mkNode("n1", 4000, 8, map[string]int{"gpu": 1}),
		mkNode("n2", 4000, 8, nil),
	}
	snap := []types.Node{nodes[0].Clone(), nodes[1].Clone()}
	_, _ = bpFit(t, nodes, types.ResourceSpec{CPUMillis: 1000}, 2)
	for i := range nodes {
		if !reflect.DeepEqual(nodes[i], snap[i]) {
			t.Fatalf("Fit mutated input node %d: %+v", i, nodes[i])
		}
	}
}
