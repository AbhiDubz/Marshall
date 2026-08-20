package sched

import (
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// BackfillScheduler implements EASY backfill: strict priority/FIFO
// order for the head of the queue, plus opportunistic backfilling of
// later jobs into idle capacity — but only when doing so provably does
// not delay the blocked head-of-queue job's reservation.
//
// The scheduler needs runtime estimates for running jobs to project
// when resources free up; those come from Lookup. Estimates are user
// supplied and may be wrong; every projection is therefore clamped so
// that an overrunning job can never push a reservation into the past
// or trick the scheduler into promising resources it does not hold.
//
// STUB — DO NOT IMPLEMENT (stubs #2 and #3). Implemented by the
// project owner: Schedule and computeReservation.
type BackfillScheduler struct {
	Alloc  Allocator
	Lookup JobLookup
}

// NewBackfillScheduler returns an EASY-backfill scheduler.
func NewBackfillScheduler(a Allocator, lookup JobLookup) *BackfillScheduler {
	return &BackfillScheduler{Alloc: a, Lookup: lookup}
}

func init() {
	register("backfill", func(a Allocator, lookup JobLookup) Scheduler {
		return NewBackfillScheduler(a, lookup)
	})
}

// Schedule implements Scheduler using EASY backfill.
//
// Algorithm (what the implementation must do):
//
//  1. Copy inputs (never mutate) and sort the queue into canonical
//     order: priority desc, submit time asc, ID asc. Equal-priority
//     ties therefore break by submit time, deterministically.
//
//  2. Walk the queue in order, starting each job whose request fits
//     right now (allocator on the working node set, charging as you
//     go), exactly like FIFO.
//
//  3. The first job that does NOT fit becomes the *blocker*. Call
//     computeReservation for it to obtain (R, S): the earliest
//     projected start time R and the node set S it will start on.
//     Only one reservation is held (that is the "EASY" in
//     EASY-backfill).
//
//  4. Continue walking the remaining queue as *backfill candidates*,
//     in the same canonical order. A candidate may start now iff:
//
//     a. it fits on the current working node set, AND
//     b. it cannot delay the reservation, meaning either
//        - now + candidate.EstRuntime <= R (it is projected to be
//          gone before the blocker needs the nodes), in which case
//          it may use any free nodes including those in S; or
//        - it can be placed entirely on nodes NOT in S (call the
//          allocator on the working set minus S), in which case its
//          runtime does not matter.
//
//     Candidates that start are charged to the working set. Note the
//     EstRuntime used in (b) is the *candidate's own estimate*; a
//     candidate that later overruns is the estimate's problem, not
//     Schedule's — see computeReservation's clamping rule for how the
//     next cycle copes.
//
//  5. Return the started allocations, all with StartAt = now. Nothing
//     is returned for the blocker (it has a reservation, not an
//     allocation) — reservations are internal projection state,
//     recomputed from scratch on every Schedule call, never carried
//     over. Recomputing every cycle is what keeps wrong estimates
//     from corrupting future decisions.
//
// Invariants the tests check:
//   - A short job that fits in a gap and is projected to finish
//     before R IS backfilled.
//   - A job that would delay the reserved job (overlaps S and outlives
//     R) is NOT backfilled.
//   - A long job that avoids S entirely IS backfilled.
//   - A running job overrunning its estimate never yields a
//     reservation in the past and never causes an allocation onto
//     resources that are not actually free.
//   - Equal-priority ties break by submit time, deterministically.
//   - Inputs (queue, nodes, running) are never mutated.
func (s *BackfillScheduler) Schedule(now time.Time, queue []types.Job, nodes []types.Node,
	running []types.Allocation) []types.Allocation {
	panic("not implemented")
}

// computeReservation projects the earliest time the blocked
// head-of-queue job `head` can start, and on which nodes.
//
// Algorithm (what the implementation must do):
//
//  1. Build the projected release schedule from `running`: for each
//     running allocation a, the job's resources on a.NodeIDs are
//     projected to release at
//
//         e(a) = max(a.StartAt + EstRuntime(a.JobID), now)
//
//     where EstRuntime comes from s.Lookup. The max(..., now) clamp is
//     the overrun rule: a job past its estimate is treated as "could
//     end any moment", so its release is projected at `now`, never in
//     the past. If Lookup cannot resolve the job, treat its release
//     time as +infinity (it never frees within the horizon) — the
//     conservative choice.
//
//  2. Walk the distinct projected release times in ascending order
//     (starting with `now` itself). At each time t, construct the
//     projected node state: current nodes, minus the allocations of
//     every running job with e(a) <= t. Ask the allocator whether
//     `head` fits (head.NodeCount nodes at head.Request).
//
//  3. The first t where it fits is the reservation: return (t, ids,
//     true) where ids is the allocator's (deterministic) node choice
//     in the projected state.
//
//  4. If head does not fit even after every running job has released
//     (i.e. the request exceeds what the cluster can ever offer given
//     current capacities and draining flags), return (zero time, nil,
//     false) — the caller then backfills without any reservation
//     constraint, since no reservation is satisfiable.
//
// Invariants the tests check:
//   - The reservation time is never before `now`.
//   - With accurate estimates, the reservation equals the actual
//     earliest feasible start.
//   - Overrunning jobs (StartAt + Est < now) clamp to `now`.
//   - The node set returned is the allocator's deterministic choice.
func (s *BackfillScheduler) computeReservation(now time.Time, head types.Job,
	nodes []types.Node, running []types.Allocation) (time.Time, []string, bool) {
	panic("not implemented")
}

func (s *BackfillScheduler) Name() string { return "backfill" }
