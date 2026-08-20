package alloc

import (
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// FirstFitAllocator picks the first nodeCount nodes (in ID order) that
// can each satisfy the request. It is the simplest correct baseline:
// O(n) per placement, no packing intelligence, and the reference
// against which bestfit and binpack are compared in the simulator.
type FirstFitAllocator struct{}

func init() { register("firstfit", func() Allocator { return FirstFitAllocator{} }) }

// Fit implements Allocator. Because the request is per-node and a job
// occupies each chosen node independently, any nodeCount distinct
// fitting nodes form a valid placement; firstfit takes the first ones
// in sorted ID order.
func (FirstFitAllocator) Fit(nodes []types.Node, req types.ResourceSpec, nodeCount int) ([]string, bool) {
	if nodeCount <= 0 {
		return nil, false
	}
	cands := candidates(nodes, req)
	if len(cands) < nodeCount {
		return nil, false
	}
	ids := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		ids[i] = cands[i].ID
	}
	return ids, true
}

func (FirstFitAllocator) Name() string { return "firstfit" }
