package alloc

import (
	"reflect"
	"testing"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

func TestBestFitPicksTightestNode(t *testing.T) {
	nodes := []types.Node{
		mkNode("n1", 8000, 16, nil),                     // loose
		withAlloc(mkNode("n2", 8000, 16, nil), 6000, 12), // 2000m/4G free: tight
		mkNode("n3", 8000, 16, nil),
	}
	ids, ok := BestFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: tightest fit", ids, ok)
	}
}

func TestBestFitExactFitWins(t *testing.T) {
	nodes := []types.Node{
		mkNode("n1", 8000, 16, nil),
		withAlloc(mkNode("n2", 8000, 16, nil), 7000, 15), // exactly 1000m/1G free
	}
	ids, ok := BestFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: exact fit is the tightest possible", ids, ok)
	}
}

func TestBestFitMultiNodeChargesEachSlot(t *testing.T) {
	// n1 can hold two slots; bestfit must not place both there because
	// the request is per node and each node is used at most once.
	nodes := []types.Node{
		mkNode("n1", 4000, 8, nil),
		mkNode("n2", 4000, 8, nil),
	}
	ids, ok := BestFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 2)
	if !ok {
		t.Fatal("expected fit")
	}
	if ids[0] == ids[1] {
		t.Fatalf("same node chosen twice: %v", ids)
	}
}

func TestBestFitCPUTieBreaksOnMemory(t *testing.T) {
	// Same CPU leftover; n2 leaves less memory behind.
	n1 := mkNode("n1", 4000, 16, nil)
	n2 := mkNode("n2", 4000, 8, nil)
	ids, ok := BestFitAllocator{}.Fit([]types.Node{n1, n2}, types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 4 << 30}, 1)
	if !ok || ids[0] != "n2" {
		t.Fatalf("got %v %v, want [n2]: less memory slack", ids, ok)
	}
}

func TestBestFitNoFit(t *testing.T) {
	nodes := []types.Node{mkNode("n1", 1000, 1, nil), mkNode("n2", 1000, 1, nil)}
	if ids, ok := (BestFitAllocator{}).Fit(nodes, types.ResourceSpec{CPUMillis: 2000}, 1); ok {
		t.Fatalf("expected no fit, got %v", ids)
	}
	if ids, ok := (BestFitAllocator{}).Fit(nodes, types.ResourceSpec{CPUMillis: 500}, 3); ok {
		t.Fatalf("expected no fit for 3 nodes, got %v", ids)
	}
}

func TestBestFitDeterministicUnderShuffle(t *testing.T) {
	base := []types.Node{
		withAlloc(mkNode("n1", 8000, 16, nil), 4000, 8),
		withAlloc(mkNode("n2", 8000, 16, nil), 4000, 8), // identical to n1: tie → lower ID
		mkNode("n3", 8000, 16, nil),
	}
	perms := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}}
	var want []string
	for i, p := range perms {
		nodes := make([]types.Node, len(base))
		for k, idx := range p {
			nodes[k] = base[idx].Clone()
		}
		ids, ok := BestFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30}, 2)
		if !ok {
			t.Fatal("expected fit")
		}
		if i == 0 {
			want = ids
			if want[0] != "n1" {
				t.Fatalf("tie must break to lower ID, got %v", want)
			}
		} else if !reflect.DeepEqual(ids, want) {
			t.Fatalf("permutation %v changed result: %v vs %v", p, ids, want)
		}
	}
}

func TestBestFitDoesNotMutateInput(t *testing.T) {
	nodes := []types.Node{mkNode("n1", 4000, 8, nil), mkNode("n2", 4000, 8, nil)}
	snap := []types.Node{nodes[0].Clone(), nodes[1].Clone()}
	_, _ = BestFitAllocator{}.Fit(nodes, types.ResourceSpec{CPUMillis: 1000}, 2)
	for i := range nodes {
		if !reflect.DeepEqual(nodes[i], snap[i]) {
			t.Fatalf("Fit mutated input node %d: %+v", i, nodes[i])
		}
	}
}
