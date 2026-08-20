package alloc

import (
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// BestFitAllocator places each requested node slot on the fitting node
// that would have the least slack remaining after placement, i.e. the
// tightest fit. This keeps large nodes free for large jobs at the cost
// of a scan per slot.
//
// Slack is scored lexicographically: leftover CPU millis first, then
// leftover memory bytes, then total leftover GRES units, with node ID
// as the final tie-break, so the choice is total and deterministic.
type BestFitAllocator struct{}

func init() { register("bestfit", func() Allocator { return BestFitAllocator{} }) }

// Fit implements Allocator. For multi-node requests it picks slots one
// at a time, charging each chosen node before picking the next, so the
// same node is never chosen twice (the request is per node).
func (BestFitAllocator) Fit(nodes []types.Node, req types.ResourceSpec, nodeCount int) ([]string, bool) {
	if nodeCount <= 0 {
		return nil, false
	}
	cands := candidates(nodes, req)
	if len(cands) < nodeCount {
		return nil, false
	}
	ids := make([]string, 0, nodeCount)
	chosen := make(map[string]bool, nodeCount)
	for slot := 0; slot < nodeCount; slot++ {
		bestIdx := -1
		for i := range cands {
			if chosen[cands[i].ID] || !cands[i].CanFit(req) {
				continue
			}
			if bestIdx == -1 || tighter(cands[i], cands[bestIdx], req) {
				bestIdx = i
			}
		}
		if bestIdx == -1 {
			return nil, false
		}
		chosen[cands[bestIdx].ID] = true
		ids = append(ids, cands[bestIdx].ID)
		cands[bestIdx].Allocated = cands[bestIdx].Allocated.Add(req)
	}
	return ids, true
}

// tighter reports whether placing req on node a leaves strictly less
// slack than placing it on node b, comparing leftover CPU, then
// leftover memory, then leftover GRES units, then ID.
func tighter(a, b types.Node, req types.ResourceSpec) bool {
	la := a.Available().Sub(req)
	lb := b.Available().Sub(req)
	if la.CPUMillis != lb.CPUMillis {
		return la.CPUMillis < lb.CPUMillis
	}
	if la.MemoryBytes != lb.MemoryBytes {
		return la.MemoryBytes < lb.MemoryBytes
	}
	if ga, gb := totalGRES(la), totalGRES(lb); ga != gb {
		return ga < gb
	}
	return a.ID < b.ID
}

func totalGRES(r types.ResourceSpec) int {
	total := 0
	for _, k := range r.GRESKinds() {
		total += r.GRES[k]
	}
	return total
}

func (BestFitAllocator) Name() string { return "bestfit" }
