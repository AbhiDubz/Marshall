package alloc

import (
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// BinPackAllocator places jobs to minimize fragmentation: it
// concentrates load onto as few nodes as possible and prefers fits
// that leave the least *unusable* slack behind, so that large jobs
// arriving later still find whole empty nodes.
type BinPackAllocator struct{}

func init() { register("binpack", func() Allocator { return BinPackAllocator{} }) }

// Fit selects nodeCount nodes that can each satisfy req, minimizing
// cluster fragmentation.
//
// Algorithm (what the implementation must do):
//
//  1. Build the candidate set: every non-draining node whose available
//     resources fit req, sorted by ID (determinism baseline).
//
//  2. Score each candidate by the state it would be left in after
//     placement, and prefer, in order:
//
//     a. EXACT FITS FIRST. If placing req consumes the node's
//     remaining resources exactly (available - req is zero in
//     every dimension, including every GRES kind), that node is
//     strictly preferred over any inexact fit: an exact fit
//     creates zero new fragments.
//
//     b. FULLER NODES FIRST. Among inexact fits, prefer the node
//     whose available capacity is smallest relative to the
//     request — i.e. pack onto already-busy nodes and keep empty
//     nodes pristine. Concretely: score by the normalized dot
//     product of leftover-after-placement across dimensions
//     (CPU, memory, each GRES kind, each normalized by node
//     capacity in that dimension); lower leftover score wins.
//     This is the classic "best fit decreasing on the aligned
//     vector" bin-packing heuristic.
//
//     c. GRES CONSERVATION. A request with no GRES must prefer a
//     node without GRES over an otherwise-equal node with free
//     GRES: burning a CPU-only job on a GPU node strands the
//     GPU. Treat free GRES on a candidate as additional leftover
//     weight when the request has none.
//
//     d. Tie-break by node ID ascending.
//
//  3. For nodeCount > 1, pick slots one at a time, charging each
//     chosen node's Allocated before scoring the next slot, so a node
//     is never chosen twice and the second slot sees the first slot's
//     effect on the cluster.
//
//  4. Return the chosen IDs in the order picked, with true; if fewer
//     than nodeCount candidates can fit (even after charging), return
//     (nil, false) and leave no visible side effects.
//
// Invariants the tests check:
//   - An exact fit is chosen over any looser fit regardless of ID order.
//   - Across a sequence of placements, binpack leaves at least one
//     whole empty node where firstfit would have smeared load over
//     every node (fragmentation avoidance).
//   - A CPU-only request never lands on a GPU node while an equal
//     CPU-only node is available.
//   - GRES requests are honored exactly (never over-committed).
//   - Multi-node requests charge earlier slots before choosing later
//     ones and never reuse a node.
//   - The result is identical under any permutation of the input
//     slice (sort before choosing), and the input slice and its nodes
//     are never mutated.
func (BinPackAllocator) Fit(nodes []types.Node, req types.ResourceSpec, nodeCount int) ([]string, bool) {
	if nodeCount <= 0 {
		return nil, false
	}
	cands := candidates(nodes, req) // clones, sorted by ID
	if len(cands) < nodeCount {
		return nil, false
	}
	ids := make([]string, 0, nodeCount)
	chosen := make(map[string]bool, nodeCount)
	for slot := 0; slot < nodeCount; slot++ {
		best := -1
		bestExact := false
		bestScore := 0.0
		for i := range cands {
			if chosen[cands[i].ID] || !cands[i].CanFit(req) {
				continue
			}
			exact, score := packScore(cands[i], req)
			if best == -1 || packBetter(exact, score, cands[i].ID, bestExact, bestScore, cands[best].ID) {
				best, bestExact, bestScore = i, exact, score
			}
		}
		if best == -1 {
			return nil, false
		}
		chosen[cands[best].ID] = true
		ids = append(ids, cands[best].ID)
		cands[best].Allocated = cands[best].Allocated.Add(req)
	}
	return ids, true
}

// packScore rates placing req on n by the state it would leave behind:
// exact fits are a class of their own (zero fragments); everything
// else scores by leftover normalized per dimension against the node's
// capacity, so free GRES a GRES-less request would strand weighs in
// automatically. Lower is better.
func packScore(n types.Node, req types.ResourceSpec) (exact bool, score float64) {
	left := n.Available().Sub(req)
	if left.IsZero() {
		return true, 0
	}
	if n.Capacity.CPUMillis > 0 {
		score += float64(left.CPUMillis) / float64(n.Capacity.CPUMillis)
	}
	if n.Capacity.MemoryBytes > 0 {
		score += float64(left.MemoryBytes) / float64(n.Capacity.MemoryBytes)
	}
	for _, k := range n.Capacity.GRESKinds() {
		if c := n.Capacity.GRES[k]; c > 0 {
			score += float64(left.GRES[k]) / float64(c)
		}
	}
	return false, score
}

// packBetter orders (exact, score, ID) lexicographically.
func packBetter(aExact bool, aScore float64, aID string, bExact bool, bScore float64, bID string) bool {
	if aExact != bExact {
		return aExact
	}
	if aScore != bScore {
		return aScore < bScore
	}
	return aID < bID
}

func (BinPackAllocator) Name() string { return "binpack" }
