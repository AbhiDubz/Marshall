package chaos

import (
	"math/rand"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

func newRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

func TestRunSeedIsDeterministic(t *testing.T) {
	cfg := Config{}
	for _, seed := range []int64{1, 7, 42} {
		a := RunSeed(cfg, seed)
		b := RunSeed(cfg, seed)
		if a.String() != b.String() {
			t.Fatalf("seed %d: nondeterministic:\n%s\nvs\n%s", seed, a, b)
		}
	}
}

func TestSmallCampaignHoldsInvariants(t *testing.T) {
	if testing.Short() {
		t.Skip("campaign is a long test")
	}
	_, failures := Campaign(Config{}, 25, nil)
	for _, f := range failures {
		t.Errorf("%s", f)
	}
	if len(failures) > 0 {
		t.Fatalf("%d/25 seeds violated invariants; replay with marshal-chaos --seed N", len(failures))
	}
}

func TestQuietSeedCompletesEverything(t *testing.T) {
	// HealBy=1 → the plan generator can schedule nothing but skew;
	// with a near-quiet cluster every job must complete well inside
	// the horizon.
	cfg := Config{HealBy: 1}
	r := RunSeed(cfg, 3)
	if r.Violation != nil {
		t.Fatalf("quiet run must pass: %v", r.Violation)
	}
}

// evilScheduler ignores capacity and dumps every pending job on the
// first node — the invariant checker must catch the double-booking.
type evilScheduler struct{}

func (evilScheduler) Schedule(now time.Time, queue []types.Job, nodes []types.Node,
	running []types.Allocation) []types.Allocation {
	if len(nodes) == 0 {
		return nil
	}
	var out []types.Allocation
	for _, j := range queue {
		if j.State != types.Pending {
			continue
		}
		ids := make([]string, max(j.NodeCount, 1))
		for i := range ids {
			ids[i] = nodes[0].ID
		}
		out = append(out, types.Allocation{JobID: j.ID, NodeIDs: ids, StartAt: now})
	}
	return out
}
func (evilScheduler) Name() string { return "evil" }

func TestCheckerCatchesDoubleBooking(t *testing.T) {
	w, err := NewWorld(Config{HealBy: 1}, 5, evilScheduler{})
	if err != nil {
		t.Fatal(err)
	}
	w.checker.seed = 5
	var got error
	for w.tick < 4000 {
		if err := w.Step(); err != nil {
			got = err
			break
		}
	}
	if got == nil {
		t.Fatal("checker never fired against a scheduler that double-books deliberately")
	}
	if v, ok := got.(*Violation); !ok || v.Invariant != "no-double-booking" {
		t.Fatalf("want a no-double-booking violation, got: %v", got)
	}
}

// stallScheduler never schedules anything: the starvation invariant
// must fire rather than the run silently idling to the horizon.
type stallScheduler struct{}

func (stallScheduler) Schedule(time.Time, []types.Job, []types.Node, []types.Allocation) []types.Allocation {
	return nil
}
func (stallScheduler) Name() string { return "stall" }

func TestCheckerCatchesStarvation(t *testing.T) {
	cfg := Config{HealBy: 1, StartBound: 5 * time.Minute}
	w, err := NewWorld(cfg, 9, stallScheduler{})
	if err != nil {
		t.Fatal(err)
	}
	w.checker.seed = 9
	var got error
	for w.tick < w.cfg.Horizon {
		if err := w.Step(); err != nil {
			got = err
			break
		}
	}
	if got == nil {
		t.Fatal("starvation invariant never fired against a scheduler that never schedules")
	}
	if v, ok := got.(*Violation); !ok || v.Invariant != "no-starvation" {
		t.Fatalf("want a no-starvation violation, got: %v", got)
	}
}

func TestGeneratePlanDeterministicAndBounded(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	p1 := GeneratePlan(newRand(11), nodes, 1000)
	p2 := GeneratePlan(newRand(11), nodes, 1000)
	if len(p1.Faults) != len(p2.Faults) {
		t.Fatal("plan generation nondeterministic")
	}
	for i := range p1.Faults {
		if p1.Faults[i].String() != p2.Faults[i].String() {
			t.Fatalf("plan differs at %d: %s vs %s", i, p1.Faults[i], p2.Faults[i])
		}
		f := p1.Faults[i]
		if f.Kind != FaultClockSkew && (f.StartTick < 0 || f.EndTick > 1000) {
			t.Fatalf("fault outside heal window: %s", f)
		}
	}
}

// TestScheduledStateNeverLeaksAcrossTicks: the chaos controller drives
// PENDING->SCHEDULED->RUNNING inside a single tick; between ticks
// nothing may sit in SCHEDULED (that would mean a placement without a
// dispatch).
func TestScheduledStateNeverLeaksAcrossTicks(t *testing.T) {
	cfg := Config{}
	scheduler, _ := sched.ByName("fifo", alloc.FirstFitAllocator{}, nil)
	w, err := NewWorld(cfg, 13, scheduler)
	if err != nil {
		t.Fatal(err)
	}
	w.checker.seed = 13
	for w.tick < 2000 && !w.allDone() {
		if err := w.Step(); err != nil {
			t.Fatal(err)
		}
		scheduled, err := w.ctl.st.ListJobs(t.Context(), types.Scheduled)
		if err != nil {
			t.Fatal(err)
		}
		if len(scheduled) != 0 {
			t.Fatalf("tick %d: %d jobs stuck in SCHEDULED", w.tick, len(scheduled))
		}
	}
}
