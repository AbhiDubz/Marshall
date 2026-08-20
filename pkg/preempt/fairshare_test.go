package preempt

import (
	"math"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

func TestFairShareDecayHalvesAtHalfLife(t *testing.T) {
	f := NewFairShare(time.Hour)
	req := types.ResourceSpec{CPUMillis: 2000}     // 2 cores
	f.Record(t0, "alice", req, 1, 100*time.Second) // 200 core-seconds

	if got := f.Usage(t0, "alice"); math.Abs(got-200) > 1e-9 {
		t.Fatalf("immediate usage: got %f want 200", got)
	}
	if got := f.Usage(t0.Add(time.Hour), "alice"); math.Abs(got-100) > 1e-6 {
		t.Fatalf("after one half-life: got %f want 100", got)
	}
	if got := f.Usage(t0.Add(2*time.Hour), "alice"); math.Abs(got-50) > 1e-6 {
		t.Fatalf("after two half-lives: got %f want 50", got)
	}
}

func TestFairShareAccumulatesAcrossRecords(t *testing.T) {
	f := NewFairShare(time.Hour)
	req := types.ResourceSpec{CPUMillis: 1000}
	f.Record(t0, "bob", req, 4, 100*time.Second)                // gang: 4 nodes -> 400 cs
	f.Record(t0.Add(time.Hour), "bob", req, 1, 100*time.Second) // +100 cs, old halved
	if got := f.Usage(t0.Add(time.Hour), "bob"); math.Abs(got-300) > 1e-6 {
		t.Fatalf("got %f want 300 (400/2 + 100)", got)
	}
}

func TestFairShareUnknownUserIsZero(t *testing.T) {
	f := NewFairShare(0)
	if got := f.Usage(t0, "nobody"); got != 0 {
		t.Fatalf("got %f want 0", got)
	}
	if p := f.Penalty(t0, "nobody"); p != 0 {
		t.Fatalf("penalty for unused account must be 0, got %f", p)
	}
}

func TestFairShareOrderDemotesHeavyUser(t *testing.T) {
	f := NewFairShare(time.Hour)
	// hog burned 100k core-seconds recently: penalty ≈ 3 levels.
	f.Record(t0, "hog", types.ResourceSpec{CPUMillis: 1000}, 1, 100000*time.Second)

	mk := func(id, user string, prio int, offset time.Duration) types.Job {
		return types.Job{ID: id, User: user, Priority: prio, SubmitAt: t0.Add(offset), State: types.Pending}
	}
	queue := []types.Job{
		mk("hog-job", "hog", 5, 0),
		mk("newbie-job", "newbie", 4, time.Second),
	}
	ordered := f.Order(t0, queue)
	if ordered[0].ID != "newbie-job" {
		t.Fatalf("decayed usage must demote the hog below a fresh prio-4 user: %v then %v",
			ordered[0].ID, ordered[1].ID)
	}
	// Input untouched.
	if queue[0].ID != "hog-job" {
		t.Fatal("Order mutated its input")
	}
}

func TestFairShareOrderTieBreaksDeterministically(t *testing.T) {
	f := NewFairShare(time.Hour)
	mk := func(id string, offset time.Duration) types.Job {
		return types.Job{ID: id, User: "u", Priority: 5, SubmitAt: t0.Add(offset), State: types.Pending}
	}
	a := f.Order(t0, []types.Job{mk("b", 0), mk("a", 0), mk("c", time.Second)})
	if a[0].ID != "a" || a[1].ID != "b" || a[2].ID != "c" {
		t.Fatalf("tie break (submit, ID) violated: %v %v %v", a[0].ID, a[1].ID, a[2].ID)
	}
}

func TestFairShareUsersSorted(t *testing.T) {
	f := NewFairShare(time.Hour)
	for _, u := range []string{"zed", "amy", "mid"} {
		f.Record(t0, u, types.ResourceSpec{CPUMillis: 1000}, 1, time.Second)
	}
	users := f.Users()
	if len(users) != 3 || users[0] != "amy" || users[1] != "mid" || users[2] != "zed" {
		t.Fatalf("got %v", users)
	}
}
