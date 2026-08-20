package store

import (
	"context"
	"os"
	"testing"
	"time"
)

func pgPoolOrSkip(t *testing.T) *PGStore {
	t.Helper()
	dsn := os.Getenv("MARSHAL_TEST_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pg, err := OpenPG(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unavailable at %s: %v", dsn, err)
	}
	t.Cleanup(pg.Close)
	return pg
}

func TestAdvisoryLockLeaderElection(t *testing.T) {
	pg := pgPoolOrSkip(t)
	ctx := context.Background()
	const key = int64(987654321) // test-private key, not LeaderKey

	l1, ok, err := TryLead(ctx, pg.Pool(), key)
	if err != nil || !ok {
		t.Fatalf("first candidate must win: ok=%v err=%v", ok, err)
	}

	// Second candidate loses while the first holds the lock.
	l2, ok, err := TryLead(ctx, pg.Pool(), key)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		l2.Release()
		t.Fatal("second candidate must not win while the lock is held")
	}

	// Release; now the second candidate wins.
	l1.Release()
	l3, err := WaitLead(ctx, pg.Pool(), key, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("candidate must win after release: %v", err)
	}
	l3.Release()
}
