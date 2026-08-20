package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// PGStore is the Postgres-backed Store used by the control plane.
type PGStore struct {
	pool *pgxpool.Pool
}

// OpenPG connects and applies pending migrations.
func OpenPG(ctx context.Context, dsn string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &PGStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Pool exposes the underlying pool for components that need raw
// access (leader election via advisory locks, the dispatch WAL).
func (s *PGStore) Pool() *pgxpool.Pool { return s.pool }

func (s *PGStore) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var done bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func gresJSON(m map[string]int) []byte {
	if m == nil {
		m = map[string]int{}
	}
	raw, _ := json.Marshal(m)
	return raw
}

func gresFromJSON(raw []byte) map[string]int {
	var m map[string]int
	_ = json.Unmarshal(raw, &m)
	if len(m) == 0 {
		return nil
	}
	return m
}

func (s *PGStore) CreateJob(ctx context.Context, job types.Job) error {
	if job.State == "" {
		job.State = types.Pending
	}
	if job.State != types.Pending {
		return fmt.Errorf("job %s: new jobs must be PENDING, got %s", job.ID, job.State)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	_, err = tx.Exec(ctx, `INSERT INTO jobs
		(id, usr, priority, cpu_millis, memory_bytes, gres, node_count, est_runtime_ms, submit_at, state, attempt)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		job.ID, job.User, job.Priority, job.Request.CPUMillis, job.Request.MemoryBytes,
		gresJSON(job.Request.GRES), max(job.NodeCount, 1), job.EstRuntime.Milliseconds(),
		job.SubmitAt, string(job.State), job.Attempt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("job %s: %w", job.ID, ErrExists)
		}
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO events (at, job_id, kind, to_state)
		VALUES ($1,$2,'job_created','PENDING')`, job.SubmitAt, job.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const jobCols = `id, usr, priority, cpu_millis, memory_bytes, gres, node_count, est_runtime_ms, submit_at, state, attempt`

func scanJob(row pgx.Row) (types.Job, error) {
	var (
		j     types.Job
		gres  []byte
		estMS int64
		state string
	)
	err := row.Scan(&j.ID, &j.User, &j.Priority, &j.Request.CPUMillis, &j.Request.MemoryBytes,
		&gres, &j.NodeCount, &estMS, &j.SubmitAt, &state, &j.Attempt)
	if err != nil {
		return types.Job{}, err
	}
	j.Request.GRES = gresFromJSON(gres)
	j.EstRuntime = time.Duration(estMS) * time.Millisecond
	j.State = types.JobState(state)
	j.SubmitAt = j.SubmitAt.UTC()
	return j, nil
}

func (s *PGStore) GetJob(ctx context.Context, id string) (types.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Job{}, fmt.Errorf("job %s: %w", id, ErrNotFound)
	}
	return j, err
}

func (s *PGStore) ListJobs(ctx context.Context, states ...types.JobState) ([]types.Job, error) {
	q := `SELECT ` + jobCols + ` FROM jobs`
	var args []any
	if len(states) > 0 {
		ss := make([]string, len(states))
		for i, st := range states {
			ss[i] = string(st)
		}
		q += ` WHERE state = ANY($1)`
		args = append(args, ss)
	}
	q += ` ORDER BY submit_at, id`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *PGStore) TransitionJob(ctx context.Context, id string, from, to types.JobState, at time.Time) (types.Job, error) {
	if err := types.ValidateTransition(from, to); err != nil {
		return types.Job{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.Job{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	bump := 0
	if requeueBump(from, to) {
		bump = 1
	}
	tag, err := tx.Exec(ctx,
		`UPDATE jobs SET state=$1, attempt=attempt+$2 WHERE id=$3 AND state=$4`,
		string(to), bump, id, string(from))
	if err != nil {
		return types.Job{}, err
	}
	if tag.RowsAffected() == 0 {
		// Distinguish missing from wrong-state for a useful error.
		var cur string
		err := tx.QueryRow(ctx, `SELECT state FROM jobs WHERE id=$1`, id).Scan(&cur)
		if errors.Is(err, pgx.ErrNoRows) {
			return types.Job{}, fmt.Errorf("job %s: %w", id, ErrNotFound)
		}
		return types.Job{}, fmt.Errorf("job %s is %s, expected %s: %w", id, cur, from, ErrConflict)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO events (at, job_id, kind, from_state, to_state)
		VALUES ($1,$2,'job_state',$3,$4)`, at, id, string(from), string(to)); err != nil {
		return types.Job{}, err
	}
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id=$1`, id))
	if err != nil {
		return types.Job{}, err
	}
	return j, tx.Commit(ctx)
}

func (s *PGStore) UpsertNode(ctx context.Context, n types.Node) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO nodes
		(id, cpu_millis, memory_bytes, gres, alloc_cpu_millis, alloc_memory_bytes, alloc_gres, last_heartbeat, draining)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET
			cpu_millis=EXCLUDED.cpu_millis, memory_bytes=EXCLUDED.memory_bytes,
			gres=EXCLUDED.gres, alloc_cpu_millis=EXCLUDED.alloc_cpu_millis,
			alloc_memory_bytes=EXCLUDED.alloc_memory_bytes, alloc_gres=EXCLUDED.alloc_gres,
			last_heartbeat=EXCLUDED.last_heartbeat, draining=EXCLUDED.draining`,
		n.ID, n.Capacity.CPUMillis, n.Capacity.MemoryBytes, gresJSON(n.Capacity.GRES),
		n.Allocated.CPUMillis, n.Allocated.MemoryBytes, gresJSON(n.Allocated.GRES),
		n.LastHeartbeat, n.Draining)
	return err
}

func (s *PGStore) ListNodes(ctx context.Context) ([]types.Node, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, cpu_millis, memory_bytes, gres,
		alloc_cpu_millis, alloc_memory_bytes, alloc_gres, last_heartbeat, draining
		FROM nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.Node
	for rows.Next() {
		var (
			n                 types.Node
			gres, allocGres   []byte
		)
		if err := rows.Scan(&n.ID, &n.Capacity.CPUMillis, &n.Capacity.MemoryBytes, &gres,
			&n.Allocated.CPUMillis, &n.Allocated.MemoryBytes, &allocGres,
			&n.LastHeartbeat, &n.Draining); err != nil {
			return nil, err
		}
		n.Capacity.GRES = gresFromJSON(gres)
		n.Allocated.GRES = gresFromJSON(allocGres)
		n.LastHeartbeat = n.LastHeartbeat.UTC()
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *PGStore) RecordAllocation(ctx context.Context, a types.Allocation, attempt int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	_, err = tx.Exec(ctx, `INSERT INTO allocations (job_id, attempt, node_ids, start_at)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (job_id, attempt) DO UPDATE SET node_ids=EXCLUDED.node_ids, start_at=EXCLUDED.start_at`,
		a.JobID, attempt, a.NodeIDs, a.StartAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return fmt.Errorf("job %s: %w", a.JobID, ErrNotFound)
		}
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO events (at, job_id, kind, detail)
		VALUES ($1,$2,'alloc',$3)`, a.StartAt, a.JobID,
		fmt.Sprintf("attempt=%d nodes=%v", attempt, a.NodeIDs)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGStore) Allocations(ctx context.Context) ([]types.Allocation, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT job_id, node_ids, start_at FROM allocations ORDER BY job_id, attempt`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.Allocation
	for rows.Next() {
		var a types.Allocation
		if err := rows.Scan(&a.JobID, &a.NodeIDs, &a.StartAt); err != nil {
			return nil, err
		}
		a.StartAt = a.StartAt.UTC()
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PGStore) AppendEvent(ctx context.Context, ev Event) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO events (at, job_id, kind, from_state, to_state, detail)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		ev.At, ev.JobID, ev.Kind, string(ev.From), string(ev.To), ev.Detail)
	return err
}

func (s *PGStore) Events(ctx context.Context, afterID int64) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, at, job_id, kind, from_state, to_state, detail
		FROM events WHERE id > $1 ORDER BY id`, afterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var (
			ev       Event
			from, to string
		)
		if err := rows.Scan(&ev.ID, &ev.At, &ev.JobID, &ev.Kind, &from, &to, &ev.Detail); err != nil {
			return nil, err
		}
		ev.From, ev.To = types.JobState(from), types.JobState(to)
		ev.At = ev.At.UTC()
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *PGStore) Close() { s.pool.Close() }
