// Package preempt decides which running jobs to evict when a pending
// high-priority job cannot otherwise be placed, and provides the
// fair-share accounting that adjusts effective priorities by decayed
// historical usage.
package preempt

import (
	"fmt"
	"sort"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Allocator mirrors alloc.Allocator; declared locally so preempt does
// not depend on a concrete allocator package.
type Allocator interface {
	Fit(nodes []types.Node, req types.ResourceSpec, nodeCount int) ([]string, bool)
	Name() string
}

// Candidate is a running job eligible for preemption consideration.
type Candidate struct {
	Job   types.Job
	Alloc types.Allocation
}

// Policy selects preemption victims.
type Policy struct {
	Alloc Allocator
}

// NewPolicy returns a preemption policy using a to verify placements.
func NewPolicy(a Allocator) *Policy { return &Policy{Alloc: a} }

// SelectVictims chooses the set of running jobs to preempt so that
// `pending` can be placed (NodeCount nodes at Request each).
//
// Algorithm:
//
//  1. ELIGIBILITY. Only candidates with Priority strictly lower than
//     pending.Priority may be victims. Equal or higher priority is
//     never preempted, no matter what. If pending already fits on the
//     current nodes without any victim (ask the allocator), return
//     ([]string{}, true): zero victims needed.
//
//  2. PER-NODE VICTIM LISTS. For each node, gather the eligible
//     candidates running (wholly or partly, for gang victims) on it,
//     sorted by: priority ascending (evict the least important
//     first), then StartAt descending (evict the job that has run
//     the shortest — least work lost), then job ID ascending.
//
//  3. GREEDY NODE SELECTION. Iteratively build the placement:
//     for each of pending.NodeCount slots, compute for every node the
//     *cost* of making room: the shortest prefix of its victim list
//     whose release (plus current free capacity) fits
//     pending.Request. Cost orders lexicographically by
//     (victim count, victim priority sum, node ID). Choose the
//     cheapest node, commit its victims (a victim already committed
//     for an earlier slot is free — a gang victim's resources release
//     on every node it occupies), and repeat with the updated
//     projection.
//
//  4. VERIFY. After committing victims, re-run the allocator on the
//     projected node state (victims' allocations released) and
//     require it to place pending. If it cannot — or if step 3 finds
//     no feasible node for some slot — return (nil, false) and select
//     nobody: a partial preemption that still cannot place pending
//     must never happen.
//
//  5. Return victim job IDs sorted ascending, with ok=true.
//
// Invariants the tests check:
//   - Minimality: the returned set is the smallest that frees enough
//     room (never evict two small jobs where one bigger one would do;
//     never evict anything when pending already fits).
//   - Never a victim with Priority >= pending.Priority.
//   - No cascade: requeued victims (strictly lower priority than the
//     preemptor by construction) can never themselves preempt the
//     preemptor or each other — SelectVictims for any victim against
//     the new running set returns no victims.
//   - Victims actually free the nodes pending lands on: every victim
//     runs on at least one node of the final placement.
//   - Deterministic under shuffled candidate and node order.
func (p *Policy) SelectVictims(pending types.Job, running []Candidate, nodes []types.Node) ([]string, bool) {
	nc := pending.NodeCount
	if nc < 1 {
		nc = 1
	}
	work := make([]types.Node, len(nodes))
	for i, n := range nodes {
		work[i] = n.Clone()
	}
	types.SortNodesByID(work)

	// Zero victims needed?
	if _, ok := p.Alloc.Fit(work, pending.Request, nc); ok {
		return []string{}, true
	}

	// Eligible: strictly lower priority only, deterministic order.
	elig := make([]Candidate, 0, len(running))
	for _, c := range running {
		if c.Job.Priority < pending.Priority {
			elig = append(elig, Candidate{Job: c.Job.Clone(), Alloc: c.Alloc.Clone()})
		}
	}
	sort.Slice(elig, func(i, k int) bool { return elig[i].Job.ID < elig[k].Job.ID })

	// onNode[n] = eligible candidates running (partly) on n, in
	// eviction order: priority asc, StartAt desc (least work lost),
	// then job ID asc.
	onNode := make(map[string][]Candidate)
	for _, c := range elig {
		for _, nid := range c.Alloc.NodeIDs {
			onNode[nid] = append(onNode[nid], c)
		}
	}
	for nid := range onNode {
		list := onNode[nid]
		sort.SliceStable(list, func(i, k int) bool {
			if list[i].Job.Priority != list[k].Job.Priority {
				return list[i].Job.Priority < list[k].Job.Priority
			}
			if !list[i].Alloc.StartAt.Equal(list[k].Alloc.StartAt) {
				return list[i].Alloc.StartAt.After(list[k].Alloc.StartAt)
			}
			return list[i].Job.ID < list[k].Job.ID
		})
	}

	committed := make(map[string]bool) // victim job IDs
	chosen := make(map[string]bool)    // node IDs claimed for pending's slots

	nodeByID := func(id string) *types.Node {
		for i := range work {
			if work[i].ID == id {
				return &work[i]
			}
		}
		return nil
	}

	for slot := 0; slot < nc; slot++ {
		bestNode := ""
		var bestPrefix []Candidate
		bestCount, bestPrio := 0, 0
		for _, n := range work {
			if chosen[n.ID] || n.Draining {
				continue
			}
			// Shortest prefix of the node's victim list (skipping
			// already-committed victims, whose resources are already
			// released in `work`) that makes pending.Request fit.
			avail := n.Available()
			var prefix []Candidate
			prioSum := 0
			feasible := pending.Request.Fits(avail)
			if !feasible {
				for _, c := range onNode[n.ID] {
					if committed[c.Job.ID] {
						continue
					}
					prefix = append(prefix, c)
					prioSum += c.Job.Priority
					avail = avail.Add(c.Job.Request)
					if pending.Request.Fits(avail) {
						feasible = true
						break
					}
				}
			}
			if !feasible {
				continue
			}
			if bestNode == "" ||
				len(prefix) < bestCount ||
				(len(prefix) == bestCount && prioSum < bestPrio) ||
				(len(prefix) == bestCount && prioSum == bestPrio && n.ID < bestNode) {
				bestNode, bestPrefix, bestCount, bestPrio = n.ID, prefix, len(prefix), prioSum
			}
		}
		if bestNode == "" {
			return nil, false
		}
		chosen[bestNode] = true
		// Commit the victims: a (gang) victim releases its request on
		// EVERY node it occupies, which may make later slots free.
		for _, v := range bestPrefix {
			committed[v.Job.ID] = true
			for _, nid := range v.Alloc.NodeIDs {
				if n := nodeByID(nid); n != nil {
					n.Allocated = n.Allocated.Sub(v.Job.Request)
				}
			}
		}
	}

	// Verify: with victims released, the allocator must place pending.
	if _, ok := p.Alloc.Fit(work, pending.Request, nc); !ok {
		return nil, false
	}
	victims := make([]string, 0, len(committed))
	for id := range committed {
		victims = append(victims, id)
	}
	sort.Strings(victims)
	return victims, true
}

// Requeue returns the job as it must re-enter the queue after
// preemption: state PENDING with Attempt incremented. The caller
// records the RUNNING -> PREEMPTED -> PENDING transitions through the
// store (which also bumps Attempt); this helper is the in-memory
// equivalent for schedulers and the simulator.
func Requeue(j types.Job) (types.Job, error) {
	if j.State != types.Preempted {
		return types.Job{}, fmt.Errorf("requeue: job %s is %s, want PREEMPTED", j.ID, j.State)
	}
	out := j.Clone()
	out.State = types.Pending
	out.Attempt++
	return out, nil
}
