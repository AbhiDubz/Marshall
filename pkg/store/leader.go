package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaderKey is the advisory-lock key marshald leaders contend on.
const LeaderKey = int64(0x6d727368616c) // "mrshal"

// Leadership is a held leader lease. Release it (or let the process
// die — Postgres drops session advisory locks with the connection,
// which is the whole point) to let another candidate take over.
type Leadership struct {
	conn    *pgxpool.Conn
	key     int64
	release func()
}

// Release gives up leadership.
func (l *Leadership) Release() {
	if l.release != nil {
		l.release()
		l.release = nil
	}
}

// TryLead attempts to become leader without blocking: it takes a
// dedicated connection from the pool and tries the session-level
// advisory lock. Returns (nil, false, nil) if someone else leads.
//
// Tradeoff (see ADR-0005): the lease lives exactly as long as the
// Postgres session. A leader that loses its DB connection loses
// leadership immediately and must stop dispatching; a partitioned
// leader cannot fence itself off — which is why every dispatch also
// goes through the WAL + CAS transitions, making a deposed leader's
// writes fail loudly instead of corrupting state.
func TryLead(ctx context.Context, pool *pgxpool.Pool, key int64) (*Leadership, bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	l := &Leadership{conn: conn, key: key}
	l.release = func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, key)
		conn.Release()
	}
	return l, true, nil
}

// WaitLead polls TryLead until it wins or ctx is done.
func WaitLead(ctx context.Context, pool *pgxpool.Pool, key int64, every time.Duration) (*Leadership, error) {
	for {
		l, ok, err := TryLead(ctx, pool, key)
		if err != nil {
			return nil, err
		}
		if ok {
			return l, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("gave up waiting for leadership: %w", ctx.Err())
		case <-time.After(every):
		}
	}
}
