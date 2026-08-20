package preempt

import (
	"math"
	"sort"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// FairShare tracks per-user resource usage with exponential decay —
// the sliding-window accounting that keeps one user from monopolizing
// the cluster by volume. Usage is measured in CPU-core-seconds and
// decays with a configurable half-life, so a burst from last week
// costs far less priority than a burst from five minutes ago.
//
// Deterministic: no wall clock — every method takes `now`, and decayed
// values are pure functions of (recorded usage, timestamps).
type FairShare struct {
	// HalfLife is the decay half-life (default 1h): after one
	// half-life, recorded usage counts half.
	HalfLife time.Duration

	usage map[string]*decayed
}

type decayed struct {
	value float64   // core-seconds, decayed as of `asOf`
	asOf  time.Time
}

// NewFairShare returns an accounting ledger with the given half-life
// (0 means the 1h default).
func NewFairShare(halfLife time.Duration) *FairShare {
	if halfLife <= 0 {
		halfLife = time.Hour
	}
	return &FairShare{HalfLife: halfLife, usage: make(map[string]*decayed)}
}

func (f *FairShare) decayTo(d *decayed, now time.Time) {
	dt := now.Sub(d.asOf)
	if dt <= 0 {
		return
	}
	d.value *= math.Exp2(-float64(dt) / float64(f.HalfLife))
	d.asOf = now
}

// Record charges a user for running `req` across nodeCount nodes for
// `dur` (e.g. called when a job finishes, or periodically for long
// jobs).
func (f *FairShare) Record(now time.Time, user string, req types.ResourceSpec, nodeCount int, dur time.Duration) {
	if nodeCount < 1 {
		nodeCount = 1
	}
	coreSeconds := float64(req.CPUMillis) / 1000 * float64(nodeCount) * dur.Seconds()
	d, ok := f.usage[user]
	if !ok {
		d = &decayed{asOf: now}
		f.usage[user] = d
	}
	f.decayTo(d, now)
	d.value += coreSeconds
}

// Usage returns the user's decayed core-seconds as of now.
func (f *FairShare) Usage(now time.Time, user string) float64 {
	d, ok := f.usage[user]
	if !ok {
		return 0
	}
	f.decayTo(d, now)
	return d.value
}

// Users returns all users with recorded usage, sorted.
func (f *FairShare) Users() []string {
	out := make([]string, 0, len(f.usage))
	for u := range f.usage {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// Order sorts a pending queue by fair-share-adjusted priority:
// (Priority - usagePenalty) descending, then submit time, then ID.
// The penalty is logarithmic in decayed usage, so heavy users lose
// whole priority levels only for order-of-magnitude differences in
// consumption; the mapping is fixed and documented rather than tuned:
//
//	penalty(u) = log10(1 + decayedCoreSeconds(u) / 100)
//
// (100 core-seconds of *recent* usage ≈ 0.3 levels; 10,000 ≈ 2 levels.)
// The input slice is not mutated; a sorted copy is returned.
func (f *FairShare) Order(now time.Time, queue []types.Job) []types.Job {
	out := make([]types.Job, len(queue))
	for i, j := range queue {
		out[i] = j.Clone()
	}
	eff := make(map[string]float64, len(out))
	for _, j := range out {
		if _, ok := eff[j.ID]; !ok {
			eff[j.ID] = float64(j.Priority) - f.Penalty(now, j.User)
		}
	}
	sort.SliceStable(out, func(i, k int) bool {
		if eff[out[i].ID] != eff[out[k].ID] {
			return eff[out[i].ID] > eff[out[k].ID]
		}
		if !out[i].SubmitAt.Equal(out[k].SubmitAt) {
			return out[i].SubmitAt.Before(out[k].SubmitAt)
		}
		return out[i].ID < out[k].ID
	})
	return out
}

// Penalty exposes the usage-to-priority penalty for observability.
func (f *FairShare) Penalty(now time.Time, user string) float64 {
	return math.Log10(1 + f.Usage(now, user)/100)
}
