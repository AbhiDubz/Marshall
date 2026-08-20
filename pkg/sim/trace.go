// Package sim is the deterministic in-memory cluster simulator: a
// virtual clock, N simulated nodes, and jobs that "run" for their real
// (not estimated) duration as recorded in the trace. Algorithms are
// developed and measured here; nothing in this package reads the wall
// clock or unseeded randomness, so a (trace, scheduler, allocator)
// triple always produces byte-identical results.
package sim

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Trace is a reproducible workload: a cluster shape plus a job stream.
// All durations are integer milliseconds so serialization is exact.
type Trace struct {
	Name    string      `json:"name"`
	Seed    int64       `json:"seed"` // seed the generator used to produce this trace
	Cluster []NodeGroup `json:"cluster"`
	Jobs    []TraceJob  `json:"jobs"`
}

// NodeGroup describes Count identical nodes.
type NodeGroup struct {
	Count       int            `json:"count"`
	CPUMillis   int64          `json:"cpu_millis"`
	MemoryBytes int64          `json:"memory_bytes"`
	GRES        map[string]int `json:"gres,omitempty"`
}

// TraceJob is one job in the workload. TrueRuntimeMS is what the job
// actually runs for in the simulator; EstRuntimeMS is what the user
// claimed, which schedulers see and may be wrong.
type TraceJob struct {
	ID             string         `json:"id"`
	User           string         `json:"user"`
	Priority       int            `json:"priority"`
	CPUMillis      int64          `json:"cpu_millis"`
	MemoryBytes    int64          `json:"memory_bytes"`
	GRES           map[string]int `json:"gres,omitempty"`
	NodeCount      int            `json:"node_count"`
	EstRuntimeMS   int64          `json:"est_runtime_ms"`
	TrueRuntimeMS  int64          `json:"true_runtime_ms"`
	SubmitOffsetMS int64          `json:"submit_offset_ms"`
}

// Epoch is the simulator's t=0. Any fixed instant works; using a
// constant keeps every printed timestamp reproducible.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Job converts the trace entry to a types.Job in Pending state.
func (tj TraceJob) Job() types.Job {
	return types.Job{
		ID:       tj.ID,
		User:     tj.User,
		Priority: tj.Priority,
		Request: types.ResourceSpec{
			CPUMillis:   tj.CPUMillis,
			MemoryBytes: tj.MemoryBytes,
			GRES:        cloneGRES(tj.GRES),
		},
		NodeCount:  max(tj.NodeCount, 1),
		EstRuntime: time.Duration(tj.EstRuntimeMS) * time.Millisecond,
		SubmitAt:   Epoch.Add(time.Duration(tj.SubmitOffsetMS) * time.Millisecond),
		State:      types.Pending,
	}
}

// Nodes materializes the cluster: node IDs are node-001, node-002, …
// in group order, so the same trace always yields the same node set.
func (t *Trace) Nodes() []types.Node {
	var nodes []types.Node
	i := 0
	for _, g := range t.Cluster {
		for k := 0; k < g.Count; k++ {
			i++
			nodes = append(nodes, types.Node{
				ID: fmt.Sprintf("node-%03d", i),
				Capacity: types.ResourceSpec{
					CPUMillis:   g.CPUMillis,
					MemoryBytes: g.MemoryBytes,
					GRES:        cloneGRES(g.GRES),
				},
				LastHeartbeat: Epoch,
			})
		}
	}
	return nodes
}

// Validate checks the trace is well formed and normalized (jobs sorted
// by submit offset then ID, unique IDs, positive durations).
func (t *Trace) Validate() error {
	if len(t.Cluster) == 0 {
		return fmt.Errorf("trace %q has no cluster definition", t.Name)
	}
	seen := make(map[string]bool, len(t.Jobs))
	for i, j := range t.Jobs {
		if j.ID == "" {
			return fmt.Errorf("job %d has empty ID", i)
		}
		if seen[j.ID] {
			return fmt.Errorf("duplicate job ID %q", j.ID)
		}
		seen[j.ID] = true
		if j.TrueRuntimeMS <= 0 || j.EstRuntimeMS <= 0 {
			return fmt.Errorf("job %q has non-positive runtime", j.ID)
		}
		if j.SubmitOffsetMS < 0 {
			return fmt.Errorf("job %q has negative submit offset", j.ID)
		}
		if i > 0 {
			prev := t.Jobs[i-1]
			if j.SubmitOffsetMS < prev.SubmitOffsetMS ||
				(j.SubmitOffsetMS == prev.SubmitOffsetMS && j.ID < prev.ID) {
				return fmt.Errorf("jobs not sorted by (submit_offset_ms, id) at index %d", i)
			}
		}
	}
	return nil
}

// normalize sorts jobs by (submit offset, ID).
func (t *Trace) normalize() {
	sort.Slice(t.Jobs, func(i, k int) bool {
		if t.Jobs[i].SubmitOffsetMS != t.Jobs[k].SubmitOffsetMS {
			return t.Jobs[i].SubmitOffsetMS < t.Jobs[k].SubmitOffsetMS
		}
		return t.Jobs[i].ID < t.Jobs[k].ID
	})
}

// Load reads and validates a trace file.
func Load(path string) (*Trace, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var t Trace
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := t.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	return &t, nil
}

// Save writes the trace as stable, indented JSON.
func (t *Trace) Save(path string) error {
	t.normalize()
	if err := t.Validate(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func cloneGRES(m map[string]int) map[string]int {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
