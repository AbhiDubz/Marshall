package sched

import (
	"sort"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// GangScheduler handles all-or-nothing multi-node jobs (NodeCount > 1)
// by *accumulating* node reservations across scheduling cycles: as
// nodes free up they are claimed and held for the highest-priority
// waiting gang job instead of being handed to whatever small job is
// next. Without this, a 4-node gang job on a busy cluster waits for
// the (probability ~0) event that 4 nodes are simultaneously free —
// i.e. it starves. With accumulation its wait is bounded by roughly
// the longest running small job, times NodeCount.
//
// Single-node jobs are scheduled greedily around the holds, so the
// held capacity is the only utilization price paid.
//
// The scheduler is stateful (the held set persists between Schedule
// calls); state is rebuilt trivially after failover because holds are
// an optimization, not a correctness requirement.
type GangScheduler struct {
	Alloc Allocator

	// held maps a gang job ID to the IDs of nodes currently being
	// held for it. Maintained exclusively by accumulate.
	held map[string][]string
}

// NewGangScheduler returns a gang scheduler.
func NewGangScheduler(a Allocator) *GangScheduler {
	return &GangScheduler{Alloc: a, held: make(map[string][]string)}
}

func init() {
	register("gang", func(a Allocator, _ JobLookup) Scheduler { return NewGangScheduler(a) })
}

// Schedule implements Scheduler. Gang jobs accumulate holds in
// priority order (only the single highest-priority pending gang job
// accumulates — one reservation at a time, like EASY); when a gang
// job's hold reaches NodeCount it starts. Single-node jobs then fill
// the remaining, un-held capacity greedily in canonical order.
func (s *GangScheduler) Schedule(now time.Time, queue []types.Job, nodes []types.Node,
	running []types.Allocation) []types.Allocation {

	work := cloneNodes(nodes)
	q := cloneQueue(queue)

	// Drop holds for jobs no longer pending (started, cancelled, …).
	pending := make(map[string]bool, len(q))
	for _, j := range q {
		if j.State == types.Pending {
			pending[j.ID] = true
		}
	}
	for id := range s.held {
		if !pending[id] {
			delete(s.held, id)
		}
	}

	var out []types.Allocation

	// Highest-priority pending gang job accumulates first.
	for _, job := range q {
		if job.State != types.Pending || !job.IsGang() {
			continue
		}
		ids, ready := s.accumulate(now, job, work)
		if ready {
			charge(work, ids, job.Request)
			delete(s.held, job.ID)
			out = append(out, types.Allocation{JobID: job.ID, NodeIDs: ids, StartAt: now})
		}
		break // only one accumulating gang reservation at a time
	}

	// Mark held nodes unavailable for everything else this cycle.
	heldSet := make(map[string]bool)
	for _, ids := range s.held {
		for _, id := range ids {
			heldSet[id] = true
		}
	}
	free := make([]types.Node, 0, len(work))
	for _, n := range work {
		if !heldSet[n.ID] {
			free = append(free, n)
		}
	}

	// Single-node jobs fill remaining capacity greedily.
	for _, job := range q {
		if job.State != types.Pending || job.IsGang() {
			continue
		}
		ids, ok := s.Alloc.Fit(free, job.Request, 1)
		if !ok {
			continue
		}
		charge(free, ids, job.Request)
		out = append(out, types.Allocation{JobID: job.ID, NodeIDs: ids, StartAt: now})
	}
	return out
}

// accumulate makes progress toward an all-or-nothing placement for the
// gang job `job`, persisting claims in s.held[job.ID] across calls.
//
// Algorithm:
//
//  1. Validate the existing hold. Every node ID already in
//     s.held[job.ID] must still exist in `work`, be non-draining, and
//     still fit job.Request on its *free* capacity (held nodes carry
//     no Allocated charge for the hold itself — the hold is the
//     scheduler refusing to place other work there, tracked purely in
//     s.held). Drop any hold entry that fails validation (the node
//     died, started draining, or a running job's resources changed).
//
//  2. Claim new nodes. Consider every node in `work` that is not in
//     any job's hold set and whose free capacity fits job.Request.
//     Claim them in deterministic order — sorted by node ID — until
//     the hold contains job.NodeCount nodes or candidates run out.
//     (Claiming greedily rather than waiting for a "better" node is
//     what bounds the gang job's wait: each claimed node is a
//     ratchet; the set only grows as running jobs finish.)
//
//  3. Persist the updated hold in s.held[job.ID], sorted by node ID.
//
//  4. If the hold now holds exactly job.NodeCount nodes, return
//     (heldIDs, true): the caller will start the job on them and
//     clear the hold. Otherwise return (heldIDs, false).
//
// Invariants the tests check:
//   - Holds are sticky: a node claimed in cycle k is still held in
//     cycle k+1 unless it became invalid (monotonic accumulation —
//     the anti-starvation ratchet).
//   - Holds never include a node that cannot currently fit the
//     request, and never overlap another gang job's hold.
//   - ready is true iff the returned set has exactly NodeCount
//     distinct valid nodes.
//   - Deterministic: same inputs and hold state produce the same
//     claims regardless of input ordering.
//   - Starvation regression: under sustained single-node load on a
//     4-node cluster, a 4-node gang job starts within a bounded
//     number of cycles (fails against any non-reserving strategy).
func (s *GangScheduler) accumulate(now time.Time, job types.Job, work []types.Node) ([]string, bool) {
	if s.held == nil {
		s.held = make(map[string][]string)
	}
	byID := make(map[string]types.Node, len(work))
	for _, n := range work {
		byID[n.ID] = n
	}
	heldByOthers := make(map[string]bool)
	for id, ids := range s.held {
		if id == job.ID {
			continue
		}
		for _, nid := range ids {
			heldByOthers[nid] = true
		}
	}

	// 1. Validate the existing hold; drop nodes that died, drain, or
	// can no longer fit the request.
	hold := make([]string, 0, job.NodeCount)
	heldSet := make(map[string]bool)
	for _, nid := range s.held[job.ID] {
		if n, ok := byID[nid]; ok && n.CanFit(job.Request) {
			hold = append(hold, nid)
			heldSet[nid] = true
		}
	}

	// 2. Claim new fitting nodes in ID order — the ratchet.
	if len(hold) < job.NodeCount {
		cands := cloneNodes(work) // sorted by ID
		for _, n := range cands {
			if len(hold) >= job.NodeCount {
				break
			}
			if heldSet[n.ID] || heldByOthers[n.ID] || !n.CanFit(job.Request) {
				continue
			}
			hold = append(hold, n.ID)
			heldSet[n.ID] = true
		}
	}

	sort.Strings(hold)
	s.held[job.ID] = hold
	out := append([]string(nil), hold...)
	return out, len(out) == job.NodeCount
}

// heldFor exposes a copy of the current hold for a job — test helper
// and observability hook (not part of the scheduling contract).
func (s *GangScheduler) heldFor(jobID string) []string {
	ids := append([]string(nil), s.held[jobID]...)
	sort.Strings(ids)
	return ids
}

func (s *GangScheduler) Name() string { return "gang" }
