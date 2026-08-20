package alloc

import (
	"reflect"
	"testing"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

func mkNode(id string, cpu, memGB int64, gres map[string]int) types.Node {
	return types.Node{
		ID:       id,
		Capacity: types.ResourceSpec{CPUMillis: cpu, MemoryBytes: memGB << 30, GRES: gres},
	}
}

func withAlloc(n types.Node, cpu, memGB int64) types.Node {
	n.Allocated = types.ResourceSpec{CPUMillis: cpu, MemoryBytes: memGB << 30}
	return n
}

func TestFirstFitSingleNode(t *testing.T) {
	nodes := []types.Node{
		withAlloc(mkNode("n1", 4000, 8, nil), 3500, 7), // 500m free
		mkNode("n2", 4000, 8, nil),                     // fully free
		mkNode("n3", 4000, 8, nil),
	}
	ids, ok := FirstFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 1)
	if !ok || !reflect.DeepEqual(ids, []string{"n2"}) {
		t.Fatalf("got %v %v, want [n2] true (n1 too full, n2 first fitting by ID)", ids, ok)
	}
}

func TestFirstFitTakesLowestIDs(t *testing.T) {
	nodes := []types.Node{
		mkNode("n3", 4000, 8, nil),
		mkNode("n1", 4000, 8, nil),
		mkNode("n2", 4000, 8, nil),
	}
	ids, ok := FirstFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 100}, 2)
	if !ok || !reflect.DeepEqual(ids, []string{"n1", "n2"}) {
		t.Fatalf("got %v %v, want [n1 n2] true", ids, ok)
	}
}

func TestFirstFitNoFit(t *testing.T) {
	nodes := []types.Node{mkNode("n1", 1000, 1, nil)}
	if ids, ok := (FirstFitAllocator{}).Fit(nodes, types.ResourceSpec{CPUMillis: 2000}, 1); ok {
		t.Fatalf("expected no fit, got %v", ids)
	}
	// Enough per-node capacity but not enough nodes.
	if ids, ok := (FirstFitAllocator{}).Fit(nodes, types.ResourceSpec{CPUMillis: 500}, 2); ok {
		t.Fatalf("expected no fit for 2 nodes, got %v", ids)
	}
	if _, ok := (FirstFitAllocator{}).Fit(nodes, types.ResourceSpec{CPUMillis: 1}, 0); ok {
		t.Fatal("nodeCount 0 must not fit")
	}
}

func TestFirstFitSkipsDrainingNodes(t *testing.T) {
	n1 := mkNode("n1", 4000, 8, nil)
	n1.Draining = true
	nodes := []types.Node{n1, mkNode("n2", 4000, 8, nil)}
	ids, ok := FirstFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 100}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: draining nodes are not candidates", ids, ok)
	}
}

func TestFirstFitRespectsGRES(t *testing.T) {
	nodes := []types.Node{
		mkNode("n1", 4000, 8, nil),
		mkNode("n2", 4000, 8, map[string]int{"gpu": 2}),
	}
	ids, ok := FirstFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 100, GRES: map[string]int{"gpu": 1}}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: only n2 has GPUs", ids, ok)
	}
}

func TestFirstFitDeterministicUnderShuffle(t *testing.T) {
	base := []types.Node{
		mkNode("n1", 4000, 8, nil), mkNode("n2", 4000, 8, nil),
		mkNode("n3", 4000, 8, nil), mkNode("n4", 4000, 8, nil),
	}
	perms := [][]int{{0, 1, 2, 3}, {3, 2, 1, 0}, {2, 0, 3, 1}, {1, 3, 0, 2}}
	var want []string
	for i, p := range perms {
		nodes := make([]types.Node, len(base))
		for k, idx := range p {
			nodes[k] = base[idx].Clone()
		}
		ids, ok := FirstFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 100}, 2)
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

func TestFirstFitDoesNotMutateInput(t *testing.T) {
	nodes := []types.Node{mkNode("n1", 4000, 8, map[string]int{"gpu": 1})}
	snapshot := nodes[0].Clone()
	_, _ = FirstFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 100, GRES: map[string]int{"gpu": 1}}, 1)
	if !reflect.DeepEqual(nodes[0], snapshot) {
		t.Fatalf("Fit mutated input node: %+v", nodes[0])
	}
}
