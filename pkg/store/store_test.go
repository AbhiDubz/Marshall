package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// The same conformance suite runs against MemStore always, and against
// PGStore when a test database is reachable (MARSHAL_TEST_DSN, or the
// default local dev container). PG-side skips are loud, not silent.

const defaultTestDSN = "postgres://marshal:marshal@localhost:5433/marshal?sslmode=disable"

func testStores(t *testing.T) map[string]Store {
	t.Helper()
	stores := map[string]Store{"mem": NewMemStore()}

	dsn := os.Getenv("MARSHAL_TEST_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pg, err := OpenPG(ctx, dsn)
	if err != nil {
		t.Logf("SKIP postgres conformance (cannot connect to %s: %v)", dsn, err)
	} else {
		// Isolate this test run.
		_, err := pg.Pool().Exec(ctx, `TRUNCATE events, allocations, jobs, nodes RESTART IDENTITY CASCADE`)
		if err != nil {
			t.Fatalf("truncate: %v", err)
		}
		stores["pg"] = pg
	}
	return stores
}

func job(id string, state types.JobState) types.Job {
	return types.Job{
		ID: id, User: "alice", Priority: 5,
		Request:    types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30, GRES: map[string]int{"gpu": 1}},
		NodeCount:  1,
		EstRuntime: time.Minute,
		SubmitAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		State:      state,
	}
}

func TestStoreConformance(t *testing.T) {
	for name, s := range testStores(t) {
		t.Run(name, func(t *testing.T) {
			defer s.Close()
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)

			// Create + fetch round-trip.
			j := job("j1", types.Pending)
			if err := s.CreateJob(ctx, j); err != nil {
				t.Fatalf("create: %v", err)
			}
			if err := s.CreateJob(ctx, j); !errors.Is(err, ErrExists) {
				t.Fatalf("duplicate create: want ErrExists, got %v", err)
			}
			got, err := s.GetJob(ctx, "j1")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.User != "alice" || got.Request.GRES["gpu"] != 1 || got.EstRuntime != time.Minute ||
				!got.SubmitAt.Equal(j.SubmitAt) || got.State != types.Pending {
				t.Fatalf("round-trip mismatch: %+v", got)
			}
			if _, err := s.GetJob(ctx, "nope"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing get: want ErrNotFound, got %v", err)
			}

			// Legal transition chain with CAS semantics.
			if _, err := s.TransitionJob(ctx, "j1", types.Pending, types.Scheduled, now); err != nil {
				t.Fatalf("PENDING->SCHEDULED: %v", err)
			}
			if _, err := s.TransitionJob(ctx, "j1", types.Pending, types.Scheduled, now); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale CAS: want ErrConflict, got %v", err)
			}
			if _, err := s.TransitionJob(ctx, "j1", types.Scheduled, types.Running, now); err != nil {
				t.Fatalf("SCHEDULED->RUNNING: %v", err)
			}
			if _, err := s.TransitionJob(ctx, "j1", types.Running, types.Completed, now); err != nil {
				t.Fatalf("RUNNING->COMPLETED: %v", err)
			}

			// Illegal edge rejected by the state machine (the brief's
			// canonical example: no COMPLETED -> RUNNING).
			if _, err := s.TransitionJob(ctx, "j1", types.Completed, types.Running, now); err == nil {
				t.Fatal("COMPLETED->RUNNING must be rejected")
			}

			// Requeue bumps Attempt.
			j2 := job("j2", types.Pending)
			if err := s.CreateJob(ctx, j2); err != nil {
				t.Fatal(err)
			}
			mustTransition(t, s, "j2", types.Pending, types.Scheduled, now)
			mustTransition(t, s, "j2", types.Scheduled, types.Running, now)
			mustTransition(t, s, "j2", types.Running, types.Preempted, now)
			upd := mustTransition(t, s, "j2", types.Preempted, types.Pending, now)
			if upd.Attempt != 1 {
				t.Fatalf("requeue must bump Attempt to 1, got %d", upd.Attempt)
			}

			// ListJobs filter + ordering.
			pend, err := s.ListJobs(ctx, types.Pending)
			if err != nil {
				t.Fatal(err)
			}
			if len(pend) != 1 || pend[0].ID != "j2" {
				t.Fatalf("pending filter wrong: %+v", pend)
			}
			all, err := s.ListJobs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(all) != 2 {
				t.Fatalf("want 2 jobs, got %d", len(all))
			}

			// Nodes.
			n := types.Node{ID: "n1",
				Capacity:      types.ResourceSpec{CPUMillis: 8000, MemoryBytes: 16 << 30, GRES: map[string]int{"gpu": 2}},
				LastHeartbeat: now}
			if err := s.UpsertNode(ctx, n); err != nil {
				t.Fatal(err)
			}
			n.Draining = true
			if err := s.UpsertNode(ctx, n); err != nil {
				t.Fatal(err)
			}
			nodes, err := s.ListNodes(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(nodes) != 1 || !nodes[0].Draining || nodes[0].Capacity.GRES["gpu"] != 2 {
				t.Fatalf("node upsert wrong: %+v", nodes)
			}

			// Allocations.
			a := types.Allocation{JobID: "j2", NodeIDs: []string{"n1"}, StartAt: now}
			if err := s.RecordAllocation(ctx, a, 0); err != nil {
				t.Fatal(err)
			}
			if err := s.RecordAllocation(ctx, types.Allocation{JobID: "ghost", NodeIDs: []string{"n1"}, StartAt: now}, 0); !errors.Is(err, ErrNotFound) {
				t.Fatalf("alloc for unknown job: want ErrNotFound, got %v", err)
			}
			allocs, err := s.Allocations(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(allocs) != 1 || allocs[0].JobID != "j2" || allocs[0].NodeIDs[0] != "n1" {
				t.Fatalf("allocations wrong: %+v", allocs)
			}

			// Event log: append-only, ordered, records the history.
			evs, err := s.Events(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(evs) == 0 {
				t.Fatal("no events logged")
			}
			var lastID int64
			var transitions int
			for _, ev := range evs {
				if ev.ID <= lastID {
					t.Fatalf("event IDs not strictly increasing: %d after %d", ev.ID, lastID)
				}
				lastID = ev.ID
				if ev.Kind == "job_state" {
					transitions++
				}
			}
			// j1: P->S->R->C (3), j2: P->S->R->PREEMPTED->P (4).
			if transitions != 7 {
				t.Fatalf("want 7 job_state events, got %d", transitions)
			}
			tail, err := s.Events(ctx, lastID-1)
			if err != nil {
				t.Fatal(err)
			}
			if len(tail) != 1 {
				t.Fatalf("afterID cursor wrong: got %d events", len(tail))
			}
		})
	}
}

func mustTransition(t *testing.T, s Store, id string, from, to types.JobState, at time.Time) types.Job {
	t.Helper()
	j, err := s.TransitionJob(context.Background(), id, from, to, at)
	if err != nil {
		t.Fatalf("%s->%s: %v", from, to, err)
	}
	return j
}

func TestMemStoreEventLogIsAppendOnlyOrdering(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := s.AppendEvent(ctx, Event{At: time.Now().UTC(), Kind: fmt.Sprintf("k%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	evs, _ := s.Events(ctx, 2)
	if len(evs) != 3 || evs[0].ID != 3 {
		t.Fatalf("cursor semantics wrong: %+v", evs)
	}
}
