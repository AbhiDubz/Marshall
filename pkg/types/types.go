// Package types defines the core data model shared by every marshal
// component: resource specifications, jobs, nodes, allocations, and the
// job state machine.
//
// Nothing in this package touches the wall clock or global randomness;
// callers supply time explicitly so that scheduling logic stays
// deterministic and testable.
package types

import (
	"fmt"
	"sort"
	"time"
)

// ResourceSpec describes a quantity of resources, either as a request
// (per node) or as a node capacity/allocation.
type ResourceSpec struct {
	CPUMillis   int64
	MemoryBytes int64
	GRES        map[string]int // generic resources, e.g. {"gpu": 2}
}

// Clone returns a deep copy of the spec (the GRES map is copied).
func (r ResourceSpec) Clone() ResourceSpec {
	out := ResourceSpec{CPUMillis: r.CPUMillis, MemoryBytes: r.MemoryBytes}
	if r.GRES != nil {
		out.GRES = make(map[string]int, len(r.GRES))
		for k, v := range r.GRES {
			out.GRES[k] = v
		}
	}
	return out
}

// Add returns r + o. Neither receiver nor argument is mutated.
func (r ResourceSpec) Add(o ResourceSpec) ResourceSpec {
	out := r.Clone()
	out.CPUMillis += o.CPUMillis
	out.MemoryBytes += o.MemoryBytes
	if len(o.GRES) > 0 && out.GRES == nil {
		out.GRES = make(map[string]int, len(o.GRES))
	}
	for k, v := range o.GRES {
		out.GRES[k] += v
	}
	return out
}

// Sub returns r - o. Neither receiver nor argument is mutated.
// Sub does not check for underflow; use Fits before subtracting.
func (r ResourceSpec) Sub(o ResourceSpec) ResourceSpec {
	out := r.Clone()
	out.CPUMillis -= o.CPUMillis
	out.MemoryBytes -= o.MemoryBytes
	if len(o.GRES) > 0 && out.GRES == nil {
		out.GRES = make(map[string]int, len(o.GRES))
	}
	for k, v := range o.GRES {
		out.GRES[k] -= v
	}
	return out
}

// Fits reports whether a request r can be satisfied by the available
// spec a, i.e. r <= a in every dimension including every GRES kind.
func (r ResourceSpec) Fits(a ResourceSpec) bool {
	if r.CPUMillis > a.CPUMillis || r.MemoryBytes > a.MemoryBytes {
		return false
	}
	for k, v := range r.GRES {
		if v > a.GRES[k] {
			return false
		}
	}
	return true
}

// IsZero reports whether the spec is empty in every dimension.
func (r ResourceSpec) IsZero() bool {
	if r.CPUMillis != 0 || r.MemoryBytes != 0 {
		return false
	}
	for _, v := range r.GRES {
		if v != 0 {
			return false
		}
	}
	return true
}

// GRESKinds returns the GRES keys in sorted order, for deterministic
// iteration.
func (r ResourceSpec) GRESKinds() []string {
	kinds := make([]string, 0, len(r.GRES))
	for k := range r.GRES {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}

// JobState is the lifecycle state of a Job.
type JobState string

const (
	Pending   JobState = "PENDING"
	Scheduled JobState = "SCHEDULED"
	Running   JobState = "RUNNING"
	Completed JobState = "COMPLETED"
	Failed    JobState = "FAILED"
	Preempted JobState = "PREEMPTED"
)

// legalTransitions is the job state machine. A job may only move along
// these edges; the store rejects anything else.
var legalTransitions = map[JobState][]JobState{
	Pending:   {Scheduled, Failed},        // -> Failed: cancellation before scheduling
	Scheduled: {Running, Pending, Failed}, // -> Pending: dispatch failed, requeue
	Running:   {Completed, Failed, Preempted},
	Preempted: {Pending}, // requeue with Attempt+1
	Failed:    {Pending}, // retry with Attempt+1
	Completed: {},        // terminal
}

// LegalTransition reports whether from -> to is a valid state machine
// edge. A self-transition is never legal.
func LegalTransition(from, to JobState) bool {
	for _, t := range legalTransitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

// ValidateTransition returns a descriptive error if from -> to is not a
// legal state machine edge.
func ValidateTransition(from, to JobState) error {
	if !LegalTransition(from, to) {
		return fmt.Errorf("illegal job state transition %s -> %s", from, to)
	}
	return nil
}

// Job is a unit of work submitted to the scheduler.
type Job struct {
	ID         string
	User       string
	Priority   int           // higher runs first
	Request    ResourceSpec  // per node
	NodeCount  int           // >1 means gang job: all-or-nothing
	EstRuntime time.Duration // user estimate, may be wrong
	SubmitAt   time.Time
	State      JobState
	Attempt    int
}

// Clone returns a deep copy of the job.
func (j Job) Clone() Job {
	out := j
	out.Request = j.Request.Clone()
	return out
}

// IsGang reports whether the job requires all-or-nothing multi-node
// placement.
func (j Job) IsGang() bool { return j.NodeCount > 1 }

// Node is a worker machine in the cluster.
type Node struct {
	ID            string
	Capacity      ResourceSpec
	Allocated     ResourceSpec
	LastHeartbeat time.Time
	Draining      bool
}

// Clone returns a deep copy of the node.
func (n Node) Clone() Node {
	out := n
	out.Capacity = n.Capacity.Clone()
	out.Allocated = n.Allocated.Clone()
	return out
}

// Available returns Capacity - Allocated.
func (n Node) Available() ResourceSpec { return n.Capacity.Sub(n.Allocated) }

// CanFit reports whether the node can accept the request right now:
// not draining and enough free resources in every dimension.
func (n Node) CanFit(req ResourceSpec) bool {
	return !n.Draining && req.Fits(n.Available())
}

// Allocation records the placement of a job on one or more nodes.
type Allocation struct {
	JobID   string
	NodeIDs []string
	StartAt time.Time
}

// Clone returns a deep copy of the allocation.
func (a Allocation) Clone() Allocation {
	out := a
	out.NodeIDs = append([]string(nil), a.NodeIDs...)
	return out
}

// SortJobsFIFO sorts jobs by priority (higher first), then submit time
// (earlier first), then ID. This is the canonical queue order used by
// every scheduler; sorting is stable and total, so any two components
// looking at the same jobs agree on order.
func SortJobsFIFO(jobs []Job) {
	sort.SliceStable(jobs, func(i, k int) bool {
		if jobs[i].Priority != jobs[k].Priority {
			return jobs[i].Priority > jobs[k].Priority
		}
		if !jobs[i].SubmitAt.Equal(jobs[k].SubmitAt) {
			return jobs[i].SubmitAt.Before(jobs[k].SubmitAt)
		}
		return jobs[i].ID < jobs[k].ID
	})
}

// SortNodesByID sorts nodes by ID, the canonical deterministic order
// for candidate selection.
func SortNodesByID(nodes []Node) {
	sort.Slice(nodes, func(i, k int) bool { return nodes[i].ID < nodes[k].ID })
}
