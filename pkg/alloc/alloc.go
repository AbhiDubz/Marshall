// Package alloc contains node-selection strategies (allocators).
//
// An Allocator answers one question: given the current nodes and a
// per-node resource request, which nodeCount nodes should the job run
// on? Allocators are pure functions of their inputs — no clocks, no
// randomness — and must be deterministic: candidates are sorted before
// choosing so that shuffled input never changes the answer.
package alloc

import (
	"sort"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Allocator selects nodeCount nodes that can each satisfy req.
// Returns the chosen node IDs and true, or nil and false if no fit exists.
// Implementations must be deterministic: sort candidates before choosing.
// Implementations must not mutate the nodes slice or its elements.
type Allocator interface {
	Fit(nodes []types.Node, req types.ResourceSpec, nodeCount int) ([]string, bool)
	Name() string
}

// candidates returns deep copies of the nodes that can currently fit
// req, sorted by ID. Copies mean callers may freely mutate (e.g. to
// track tentative placement for multi-node fits) without touching the
// caller's slice.
func candidates(nodes []types.Node, req types.ResourceSpec) []types.Node {
	out := make([]types.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.CanFit(req) {
			out = append(out, n.Clone())
		}
	}
	types.SortNodesByID(out)
	return out
}

// registry maps allocator names to constructors. Each implementation
// file registers itself in init(), so the registry always matches what
// is compiled in.
var registry = map[string]func() Allocator{}

func register(name string, f func() Allocator) { registry[name] = f }

// ByName returns the allocator registered under name, or false.
func ByName(name string) (Allocator, bool) {
	f, ok := registry[name]
	if !ok {
		return nil, false
	}
	return f(), true
}

// Names lists the registered allocator names in sorted order.
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
