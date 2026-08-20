package sched

import (
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// FIFOScheduler starts jobs strictly in canonical queue order
// (priority desc, then submit time, then ID) and blocks at the first
// job that does not fit — classic head-of-line blocking. It never
// skips ahead, which makes it the baseline that backfill improves on:
// any utilization backfill wins over FIFO comes purely from filling
// the holes FIFO leaves while the head waits.
type FIFOScheduler struct {
	Alloc Allocator
}

// Allocator is the node-selection strategy the scheduler delegates
// placement to. It mirrors alloc.Allocator; declared locally so sched
// does not import alloc (avoids a dependency cycle when alloc tests
// use schedulers).
type Allocator interface {
	Fit(nodes []types.Node, req types.ResourceSpec, nodeCount int) ([]string, bool)
	Name() string
}

// NewFIFOScheduler returns a FIFO scheduler using a for placement.
func NewFIFOScheduler(a Allocator) *FIFOScheduler { return &FIFOScheduler{Alloc: a} }

func init() {
	register("fifo", func(a Allocator, _ JobLookup) Scheduler { return NewFIFOScheduler(a) })
}

// Schedule implements Scheduler.
func (s *FIFOScheduler) Schedule(now time.Time, queue []types.Job, nodes []types.Node,
	running []types.Allocation) []types.Allocation {

	work := cloneNodes(nodes)
	q := cloneQueue(queue)

	var out []types.Allocation
	for _, job := range q {
		if job.State != types.Pending {
			continue
		}
		nodeCount := job.NodeCount
		if nodeCount == 0 {
			nodeCount = 1
		}
		ids, ok := s.Alloc.Fit(work, job.Request, nodeCount)
		if !ok {
			break // strict FIFO: head-of-line blocks everything behind it
		}
		charge(work, ids, job.Request)
		out = append(out, types.Allocation{JobID: job.ID, NodeIDs: ids, StartAt: now})
	}
	return out
}

func (s *FIFOScheduler) Name() string { return "fifo" }
