// Package sched contains scheduling policies.
//
// A Scheduler decides, at a given instant, which pending jobs to start
// and where. It is a pure decision function over the inputs it is
// handed — the wall clock is never consulted (the caller passes `now`),
// randomness is never used, and inputs are never mutated. Schedulers
// that need to reason about the future (backfill, gang) additionally
// need runtime estimates for *running* jobs; those are supplied via a
// JobLookup injected at construction, since the Schedule signature only
// carries pending jobs.
package sched

import (
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Scheduler decides what to dispatch at time `now`.
// queue is sorted by submit time; the scheduler applies its own priority rules.
// Returns allocations to start immediately. Must not mutate its inputs.
type Scheduler interface {
	Schedule(now time.Time, queue []types.Job, nodes []types.Node,
		running []types.Allocation) []types.Allocation
	Name() string
}

// JobLookup resolves a job ID to its Job record (including EstRuntime
// and SubmitAt) for jobs the scheduler is not currently being handed in
// the queue — i.e. running jobs. The second result is false if the ID
// is unknown.
type JobLookup func(jobID string) (types.Job, bool)

// cloneNodes deep-copies a node slice and sorts it by ID; the working
// set every scheduler mutates internally instead of its inputs.
func cloneNodes(nodes []types.Node) []types.Node {
	out := make([]types.Node, len(nodes))
	for i, n := range nodes {
		out[i] = n.Clone()
	}
	types.SortNodesByID(out)
	return out
}

// cloneQueue copies the pending queue and sorts it into canonical
// order: priority desc, submit asc, ID asc.
func cloneQueue(queue []types.Job) []types.Job {
	out := make([]types.Job, len(queue))
	for i, j := range queue {
		out[i] = j.Clone()
	}
	types.SortJobsFIFO(out)
	return out
}

// charge applies req to the named nodes in the working set.
func charge(nodes []types.Node, ids []string, req types.ResourceSpec) {
	for _, id := range ids {
		for i := range nodes {
			if nodes[i].ID == id {
				nodes[i].Allocated = nodes[i].Allocated.Add(req)
			}
		}
	}
}
