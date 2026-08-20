package chaos

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

func newSeedRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

// Result is one seed's outcome.
type Result struct {
	Seed      int64
	Ticks     int
	Faults    int
	Violation error // nil = all invariants held and every job completed
}

func (r Result) String() string {
	if r.Violation == nil {
		return fmt.Sprintf("seed %d: PASS (%d ticks, %d faults)", r.Seed, r.Ticks, r.Faults)
	}
	return fmt.Sprintf("seed %d: FAIL: %v", r.Seed, r.Violation)
}

// RunSeed executes one fully deterministic chaos run: same (cfg,
// seed), same result, byte for byte.
func RunSeed(cfg Config, seed int64) Result {
	cfg.defaults()
	a, ok := alloc.ByName(cfg.Allocator)
	if !ok {
		return Result{Seed: seed, Violation: fmt.Errorf("unknown allocator %q", cfg.Allocator)}
	}

	// The scheduler's JobLookup resolves from the world's store; the
	// closure binds the pointer before the world exists (two-phase).
	var w *World
	lookup := sched.JobLookup(func(id string) (types.Job, bool) {
		if w == nil {
			return types.Job{}, false
		}
		j, err := w.ctl.st.GetJob(context.Background(), id)
		return j, err == nil
	})
	scheduler, ok := sched.ByName(cfg.Scheduler, a, lookup)
	if !ok {
		return Result{Seed: seed, Violation: fmt.Errorf("unknown scheduler %q", cfg.Scheduler)}
	}

	var err error
	w, err = NewWorld(cfg, seed, scheduler)
	if err != nil {
		return Result{Seed: seed, Violation: err}
	}
	w.checker.seed = seed

	res := Result{Seed: seed, Faults: len(w.plan.Faults)}
	defer func() { res.Ticks = w.tick }()

	for w.tick < cfg.Horizon {
		if err := w.Step(); err != nil {
			res.Violation = err
			res.Ticks = w.tick
			return res
		}
		if w.allDone() {
			break
		}
	}
	res.Ticks = w.tick
	if err := w.checker.Final(w); err != nil {
		res.Violation = err
	}
	return res
}

func (w *World) allDone() bool {
	jobs, err := w.ctl.st.ListJobs(context.Background())
	if err != nil || len(jobs) != len(w.jobs) {
		return false
	}
	for _, j := range jobs {
		if j.State != types.Completed {
			return false
		}
	}
	return true
}

// DescribeSeed renders a seed's fault plan (for --seed replays).
func DescribeSeed(cfg Config, seed int64) string {
	cfg.defaults()
	var nodes []string
	for i := 1; i <= cfg.Nodes; i++ {
		nodes = append(nodes, fmt.Sprintf("node-%02d", i))
	}
	plan := GeneratePlan(newSeedRand(seed), nodes, cfg.HealBy)
	out := fmt.Sprintf("seed %d: %d faults\n", seed, len(plan.Faults))
	for _, f := range plan.Faults {
		out += "  " + f.String() + "\n"
	}
	return out
}

// Campaign runs seeds 1..n and returns every result plus the failures.
func Campaign(cfg Config, n int, progress func(Result)) (results []Result, failures []Result) {
	for seed := int64(1); seed <= int64(n); seed++ {
		r := RunSeed(cfg, seed)
		results = append(results, r)
		if r.Violation != nil {
			failures = append(failures, r)
		}
		if progress != nil {
			progress(r)
		}
	}
	return results, failures
}
