// Package dispatch implements the exactly-once job dispatch protocol:
// a write-ahead log records intent before any external effect and
// commit after the agent accepts, so a control plane that dies at any
// point can be replaced by a new leader that reconciles without losing
// the job or running it twice.
package dispatch

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RecordKind labels WAL records.
type RecordKind string

const (
	KindIntent RecordKind = "DISPATCH_INTENT"
	KindCommit RecordKind = "DISPATCH_COMMIT"
	KindAbort  RecordKind = "DISPATCH_ABORT"
)

// Record is one WAL entry.
type Record struct {
	LSN     int64
	Kind    RecordKind
	Token   string
	JobID   string
	Attempt int
	NodeIDs []string
	At      time.Time
}

// WAL is an append-only, durably ordered log. Duplicate records for a
// token are legal (a retried append whose ack was lost); readers
// deduplicate by (kind, token).
type WAL interface {
	Append(ctx context.Context, rec Record) (int64, error)
	Scan(ctx context.Context) ([]Record, error)
}

// MemWAL is the in-memory WAL for tests and the chaos harness.
type MemWAL struct {
	mu   sync.Mutex
	log  []Record
	next int64
}

func NewMemWAL() *MemWAL { return &MemWAL{} }

func (w *MemWAL) Append(_ context.Context, rec Record) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.next++
	rec.LSN = w.next
	w.log = append(w.log, rec)
	return rec.LSN, nil
}

func (w *MemWAL) Scan(_ context.Context) ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Record, len(w.log))
	copy(out, w.log)
	sort.Slice(out, func(i, k int) bool { return out[i].LSN < out[k].LSN })
	return out, nil
}

// PGWAL stores records in the dispatch_wal table (migration 0003).
type PGWAL struct {
	pool *pgxpool.Pool
}

func NewPGWAL(pool *pgxpool.Pool) *PGWAL { return &PGWAL{pool: pool} }

func (w *PGWAL) Append(ctx context.Context, rec Record) (int64, error) {
	var lsn int64
	err := w.pool.QueryRow(ctx,
		`INSERT INTO dispatch_wal (kind, token, job_id, attempt, node_ids, at)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING lsn`,
		string(rec.Kind), rec.Token, rec.JobID, rec.Attempt, rec.NodeIDs, rec.At).Scan(&lsn)
	return lsn, err
}

func (w *PGWAL) Scan(ctx context.Context) ([]Record, error) {
	rows, err := w.pool.Query(ctx,
		`SELECT lsn, kind, token, job_id, attempt, node_ids, at FROM dispatch_wal ORDER BY lsn`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var (
			r    Record
			kind string
		)
		if err := rows.Scan(&r.LSN, &kind, &r.Token, &r.JobID, &r.Attempt, &r.NodeIDs, &r.At); err != nil {
			return nil, err
		}
		r.Kind = RecordKind(kind)
		r.At = r.At.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}
