package sim

import (
	"fmt"
	"math"
	"math/rand"
)

// Generate produces a reproducible workload trace. Every random choice
// comes from a single *rand.Rand seeded with `seed`; the same
// (kind, seed, jobs) triple always yields an identical trace.
//
// Kinds:
//   - uniform: steady stream of single-node jobs, exponential
//     interarrivals, mixed sizes and priorities.
//   - bimodal: many small short jobs plus a minority of large,
//     long-running gang jobs (some needing GPUs) on a cluster with a
//     GPU partition.
//   - bursty: quiet baseline punctuated by tight submission bursts.
func Generate(kind string, seed int64, jobs int) (*Trace, error) {
	rng := rand.New(rand.NewSource(seed))
	t := &Trace{Name: kind, Seed: seed}
	switch kind {
	case "uniform":
		t.Cluster = []NodeGroup{{Count: 8, CPUMillis: 8000, MemoryBytes: 16 << 30}}
		genUniform(rng, t, jobs)
	case "bimodal":
		t.Cluster = []NodeGroup{
			{Count: 8, CPUMillis: 8000, MemoryBytes: 32 << 30},
			{Count: 4, CPUMillis: 8000, MemoryBytes: 32 << 30, GRES: map[string]int{"gpu": 4}},
		}
		genBimodal(rng, t, jobs)
	case "bursty":
		t.Cluster = []NodeGroup{{Count: 8, CPUMillis: 8000, MemoryBytes: 16 << 30}}
		genBursty(rng, t, jobs)
	default:
		return nil, fmt.Errorf("unknown trace kind %q (want uniform, bimodal, or bursty)", kind)
	}
	t.normalize()
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return t, nil
}

var users = []string{"alice", "bob", "carol", "dave", "erin"}

// pickPriority draws 0 (60%), 5 (30%), or 9 (10%).
func pickPriority(rng *rand.Rand) int {
	switch p := rng.Float64(); {
	case p < 0.6:
		return 0
	case p < 0.9:
		return 5
	default:
		return 9
	}
}

// trueFromEst perturbs an estimate into an actual runtime: a lognormal
// factor clamped to [0.25, 3.0], so estimates are wrong in both
// directions but never absurd.
func trueFromEst(rng *rand.Rand, estMS int64) int64 {
	factor := math.Exp(rng.NormFloat64() * 0.4)
	factor = math.Max(0.25, math.Min(3.0, factor))
	out := int64(float64(estMS) * factor)
	return max(out, 1000)
}

func genUniform(rng *rand.Rand, t *Trace, jobs int) {
	offset := int64(0)
	cpus := []int64{500, 1000, 2000, 4000}
	for i := 0; i < jobs; i++ {
		offset += int64(rng.ExpFloat64() * 15000) // mean 15s interarrival
		est := int64(60000 + rng.Intn(540000))    // 1–10 min
		t.Jobs = append(t.Jobs, TraceJob{
			ID:             fmt.Sprintf("job-%04d", i+1),
			User:           users[rng.Intn(len(users))],
			Priority:       pickPriority(rng),
			CPUMillis:      cpus[rng.Intn(len(cpus))],
			MemoryBytes:    int64(1+rng.Intn(8)) << 30,
			NodeCount:      1,
			EstRuntimeMS:   est,
			TrueRuntimeMS:  trueFromEst(rng, est),
			SubmitOffsetMS: offset,
		})
	}
}

func genBimodal(rng *rand.Rand, t *Trace, jobs int) {
	offset := int64(0)
	for i := 0; i < jobs; i++ {
		offset += int64(rng.ExpFloat64() * 20000) // mean 20s interarrival
		j := TraceJob{
			ID:             fmt.Sprintf("job-%04d", i+1),
			User:           users[rng.Intn(len(users))],
			SubmitOffsetMS: offset,
		}
		if rng.Float64() < 0.85 {
			// Small short job.
			j.Priority = pickPriority(rng)
			j.CPUMillis = []int64{500, 1000}[rng.Intn(2)]
			j.MemoryBytes = int64(1+rng.Intn(4)) << 30
			j.NodeCount = 1
			j.EstRuntimeMS = int64(30000 + rng.Intn(90000)) // 0.5–2 min
		} else {
			// Large gang job; 30% of them need GPUs.
			j.Priority = 5 + rng.Intn(5)
			j.CPUMillis = 4000
			j.MemoryBytes = int64(8+rng.Intn(8)) << 30
			j.NodeCount = 2 + rng.Intn(3)                       // 2–4 nodes
			j.EstRuntimeMS = int64(600000 + rng.Intn(1200000)) // 10–30 min
			if rng.Float64() < 0.3 {
				j.GRES = map[string]int{"gpu": 1 + rng.Intn(2)}
			}
		}
		j.TrueRuntimeMS = trueFromEst(rng, j.EstRuntimeMS)
		t.Jobs = append(t.Jobs, j)
	}
}

func genBursty(rng *rand.Rand, t *Trace, jobs int) {
	offset := int64(0)
	i := 0
	for i < jobs {
		// Quiet gap, then a burst of 15–40 near-simultaneous submits.
		offset += int64(120000 + rng.Intn(480000)) // 2–10 min of quiet
		burst := 15 + rng.Intn(26)
		for b := 0; b < burst && i < jobs; b++ {
			est := int64(60000 + rng.Intn(240000)) // 1–5 min
			t.Jobs = append(t.Jobs, TraceJob{
				ID:             fmt.Sprintf("job-%04d", i+1),
				User:           users[rng.Intn(len(users))],
				Priority:       pickPriority(rng),
				CPUMillis:      []int64{1000, 2000, 4000}[rng.Intn(3)],
				MemoryBytes:    int64(1+rng.Intn(8)) << 30,
				NodeCount:      1,
				EstRuntimeMS:   est,
				TrueRuntimeMS:  trueFromEst(rng, est),
				SubmitOffsetMS: offset + int64(rng.Intn(2000)), // within 2s of the burst
			})
			i++
		}
	}
}
