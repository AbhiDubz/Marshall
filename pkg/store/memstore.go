package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// MemStore is the in-memory Store used by the simulator, the chaos
// harness, and tests. Semantics mirror PGStore exactly; the shared
// conformance suite in store_test.go keeps them honest.
type MemStore struct {
	mu     sync.Mutex
	jobs   map[string]types.Job
	nodes  map[string]types.Node
	allocs map[string]map[int]types.Allocation // jobID -> attempt -> alloc
	events []Event
	nextID int64
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		jobs:   make(map[string]types.Job),
		nodes:  make(map[string]types.Node),
		allocs: make(map[string]map[int]types.Allocation),
	}
}

func (m *MemStore) appendLocked(ev Event) {
	m.nextID++
	ev.ID = m.nextID
	m.events = append(m.events, ev)
}

func (m *MemStore) CreateJob(_ context.Context, job types.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[job.ID]; ok {
		return fmt.Errorf("job %s: %w", job.ID, ErrExists)
	}
	if job.State == "" {
		job.State = types.Pending
	}
	if job.State != types.Pending {
		return fmt.Errorf("job %s: new jobs must be PENDING, got %s", job.ID, job.State)
	}
	m.jobs[job.ID] = job.Clone()
	m.appendLocked(Event{At: job.SubmitAt, JobID: job.ID, Kind: "job_created", To: types.Pending})
	return nil
}

func (m *MemStore) GetJob(_ context.Context, id string) (types.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return types.Job{}, fmt.Errorf("job %s: %w", id, ErrNotFound)
	}
	return j.Clone(), nil
}

func (m *MemStore) ListJobs(_ context.Context, states ...types.JobState) ([]types.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	want := make(map[types.JobState]bool, len(states))
	for _, s := range states {
		want[s] = true
	}
	var out []types.Job
	for _, j := range m.jobs {
		if len(want) == 0 || want[j.State] {
			out = append(out, j.Clone())
		}
	}
	sort.Slice(out, func(i, k int) bool {
		if !out[i].SubmitAt.Equal(out[k].SubmitAt) {
			return out[i].SubmitAt.Before(out[k].SubmitAt)
		}
		return out[i].ID < out[k].ID
	})
	return out, nil
}

func (m *MemStore) TransitionJob(_ context.Context, id string, from, to types.JobState, at time.Time) (types.Job, error) {
	if err := types.ValidateTransition(from, to); err != nil {
		return types.Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return types.Job{}, fmt.Errorf("job %s: %w", id, ErrNotFound)
	}
	if j.State != from {
		return types.Job{}, fmt.Errorf("job %s is %s, expected %s: %w", id, j.State, from, ErrConflict)
	}
	j.State = to
	if requeueBump(from, to) {
		j.Attempt++
	}
	m.jobs[id] = j
	m.appendLocked(Event{At: at, JobID: id, Kind: "job_state", From: from, To: to})
	return j.Clone(), nil
}

func (m *MemStore) UpsertNode(_ context.Context, node types.Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node.Clone()
	return nil
}

func (m *MemStore) ListNodes(_ context.Context) ([]types.Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]types.Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, n.Clone())
	}
	types.SortNodesByID(out)
	return out, nil
}

func (m *MemStore) RecordAllocation(_ context.Context, a types.Allocation, attempt int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[a.JobID]; !ok {
		return fmt.Errorf("job %s: %w", a.JobID, ErrNotFound)
	}
	if m.allocs[a.JobID] == nil {
		m.allocs[a.JobID] = make(map[int]types.Allocation)
	}
	m.allocs[a.JobID][attempt] = a.Clone()
	m.appendLocked(Event{At: a.StartAt, JobID: a.JobID, Kind: "alloc",
		Detail: fmt.Sprintf("attempt=%d nodes=%v", attempt, a.NodeIDs)})
	return nil
}

func (m *MemStore) Allocations(_ context.Context) ([]types.Allocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type key struct {
		job     string
		attempt int
	}
	var keys []key
	for job, byAttempt := range m.allocs {
		for at := range byAttempt {
			keys = append(keys, key{job, at})
		}
	}
	sort.Slice(keys, func(i, k int) bool {
		if keys[i].job != keys[k].job {
			return keys[i].job < keys[k].job
		}
		return keys[i].attempt < keys[k].attempt
	})
	out := make([]types.Allocation, 0, len(keys))
	for _, k := range keys {
		out = append(out, m.allocs[k.job][k.attempt].Clone())
	}
	return out, nil
}

func (m *MemStore) AppendEvent(_ context.Context, ev Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendLocked(ev)
	return nil
}

func (m *MemStore) Events(_ context.Context, afterID int64) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	for _, ev := range m.events {
		if ev.ID > afterID {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (m *MemStore) Close() {}
