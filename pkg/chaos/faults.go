// Package chaos runs the whole control loop — controller, agents,
// network — as a deterministic discrete-time simulation under seeded
// fault injection, with a continuous invariant checker. Any failing
// seed replays exactly: the entire run is a pure function of (config,
// seed).
package chaos

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// FaultKind enumerates the injectable faults.
type FaultKind string

const (
	// FaultKill crashes the agent process: executions are lost, no
	// heartbeats until revival, node returns empty.
	FaultKill FaultKind = "kill"
	// FaultPartition drops all traffic between agent and controller;
	// the agent keeps running its jobs (zombies from the controller's
	// point of view). Queued completions deliver on heal.
	FaultPartition FaultKind = "partition"
	// FaultPause SIGSTOPs the agent: no heartbeats, no job progress,
	// executions keep holding node resources.
	FaultPause FaultKind = "pause"
	// FaultHBDrop drops each heartbeat in the window with probability
	// DropP.
	FaultHBDrop FaultKind = "hb-drop"
	// FaultHBDelay delays each heartbeat in the window by DelayTicks.
	FaultHBDelay FaultKind = "hb-delay"
	// FaultClockSkew multiplies the agent's notion of a second by
	// SkewFactor for the whole run (a fast clock heartbeats early, a
	// slow one late — late enough and the failure detector trips).
	FaultClockSkew FaultKind = "clock-skew"
)

// Fault is one scheduled fault against one node.
type Fault struct {
	Kind       FaultKind
	Node       string
	StartTick  int
	EndTick    int     // exclusive; ignored for kill (RecoverTick) and skew
	DropP      float64 // hb-drop
	DelayTicks int     // hb-delay
	SkewFactor float64 // clock-skew
}

func (f Fault) String() string {
	switch f.Kind {
	case FaultClockSkew:
		return fmt.Sprintf("%s(%s x%.2f)", f.Kind, f.Node, f.SkewFactor)
	case FaultHBDrop:
		return fmt.Sprintf("%s(%s p=%.2f [%d,%d))", f.Kind, f.Node, f.DropP, f.StartTick, f.EndTick)
	case FaultHBDelay:
		return fmt.Sprintf("%s(%s +%dt [%d,%d))", f.Kind, f.Node, f.DelayTicks, f.StartTick, f.EndTick)
	default:
		return fmt.Sprintf("%s(%s [%d,%d))", f.Kind, f.Node, f.StartTick, f.EndTick)
	}
}

// Plan is a seed's full fault schedule, sorted by StartTick then node.
type Plan struct {
	Faults []Fault
}

// GeneratePlan draws a fault schedule from rng. All faults start and
// end inside [0, healBy): after healBy the cluster is quiet so every
// run can converge before the horizon.
func GeneratePlan(rng *rand.Rand, nodes []string, healBy int) Plan {
	var p Plan
	add := func(f Fault) { p.Faults = append(p.Faults, f) }
	// A heal window too small to hold a fault yields a (nearly) quiet
	// plan: only clock skew, which has no window.
	windowed := healBy >= 8
	window := func(maxLen int) (int, int) {
		start := rng.Intn(healBy * 3 / 4)
		length := 1 + rng.Intn(maxLen)
		end := start + length
		if end > healBy {
			end = healBy
		}
		return start, end
	}

	for _, n := range nodes {
		// Each node independently draws each fault type with some
		// probability, so seeds range from quiet to brutal.
		if windowed && rng.Float64() < 0.35 {
			s, e := window(120) // up to a minute of downtime at 500ms ticks
			add(Fault{Kind: FaultKill, Node: n, StartTick: s, EndTick: e})
		}
		if windowed && rng.Float64() < 0.35 {
			s, e := window(120)
			add(Fault{Kind: FaultPartition, Node: n, StartTick: s, EndTick: e})
		}
		if windowed && rng.Float64() < 0.25 {
			s, e := window(80)
			add(Fault{Kind: FaultPause, Node: n, StartTick: s, EndTick: e})
		}
		if windowed && rng.Float64() < 0.3 {
			s, e := window(200)
			add(Fault{Kind: FaultHBDrop, Node: n, StartTick: s, EndTick: e, DropP: 0.3 + rng.Float64()*0.6})
		}
		if windowed && rng.Float64() < 0.3 {
			s, e := window(200)
			add(Fault{Kind: FaultHBDelay, Node: n, StartTick: s, EndTick: e, DelayTicks: 1 + rng.Intn(8)})
		}
		if rng.Float64() < 0.2 {
			add(Fault{Kind: FaultClockSkew, Node: n, SkewFactor: 0.5 + rng.Float64()*1.5})
		}
	}
	sort.SliceStable(p.Faults, func(i, k int) bool {
		if p.Faults[i].StartTick != p.Faults[k].StartTick {
			return p.Faults[i].StartTick < p.Faults[k].StartTick
		}
		if p.Faults[i].Node != p.Faults[k].Node {
			return p.Faults[i].Node < p.Faults[k].Node
		}
		return p.Faults[i].Kind < p.Faults[k].Kind
	})
	return p
}

// Tick is the simulation quantum.
const Tick = 500 * time.Millisecond
