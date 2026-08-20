// Package store persists jobs, nodes, allocations, and an append-only
// event log. Two implementations share one contract (and one
// conformance test suite): MemStore for the simulator, chaos harness,
// and unit tests; PGStore for the real control plane.
//
// Every job state change goes through TransitionJob, which validates
// the edge against the types state machine — an illegal transition
// (COMPLETED -> RUNNING) is rejected no matter who asks — and appends
// an event recording it.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

var (
	// ErrNotFound is returned when the requested row does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrConflict is returned when a compare-and-swap transition loses:
	// the job was not in the expected `from` state.
	ErrConflict = errors.New("store: state conflict")
	// ErrExists is returned when creating a row that already exists.
	ErrExists = errors.New("store: already exists")
)

// Event is one record in the append-only log.
type Event struct {
	ID    int64
	At    time.Time
	JobID string
	Kind  string // "job_created", "job_state", "node_upsert", "alloc", ...
	From  types.JobState
	To    types.JobState
	Detail string
}

// Store is the persistence contract.
type Store interface {
	// CreateJob inserts a new job (must be PENDING) and logs a
	// job_created event. ErrExists if the ID is taken.
	CreateJob(ctx context.Context, job types.Job) error

	// GetJob fetches one job. ErrNotFound if absent.
	GetJob(ctx context.Context, id string) (types.Job, error)

	// ListJobs returns jobs in the given states (all states if empty),
	// sorted by (SubmitAt, ID).
	ListJobs(ctx context.Context, states ...types.JobState) ([]types.Job, error)

	// TransitionJob moves a job from -> to atomically (compare-and-swap
	// on `from`), validating the edge against the state machine and
	// appending a job_state event. Requeue edges (PREEMPTED->PENDING,
	// FAILED->PENDING, SCHEDULED->PENDING) increment Attempt.
	// Returns the updated job. ErrConflict if the job was not in
	// `from`; a validation error if the edge is illegal.
	TransitionJob(ctx context.Context, id string, from, to types.JobState, at time.Time) (types.Job, error)

	// UpsertNode inserts or replaces a node row.
	UpsertNode(ctx context.Context, node types.Node) error

	// ListNodes returns all nodes sorted by ID.
	ListNodes(ctx context.Context) ([]types.Node, error)

	// RecordAllocation stores the placement for (JobID, attempt).
	RecordAllocation(ctx context.Context, a types.Allocation, attempt int) error

	// Allocations returns all recorded allocations sorted by
	// (JobID, attempt).
	Allocations(ctx context.Context) ([]types.Allocation, error)

	// AppendEvent adds an arbitrary event to the log.
	AppendEvent(ctx context.Context, ev Event) error

	// Events returns log entries with ID > afterID, oldest first.
	Events(ctx context.Context, afterID int64) ([]Event, error)

	Close()
}

// requeueBump reports whether the transition increments Attempt.
func requeueBump(from, to types.JobState) bool {
	if to != types.Pending {
		return false
	}
	return from == types.Preempted || from == types.Failed || from == types.Scheduled
}
