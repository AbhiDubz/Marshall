package sched

import "sort"

// registry maps scheduler names to constructors. Each implementation
// file registers itself in init(), so the registry always matches what
// is compiled in.
var registry = map[string]func(a Allocator, lookup JobLookup) Scheduler{}

func register(name string, f func(a Allocator, lookup JobLookup) Scheduler) {
	registry[name] = f
}

// ByName constructs the scheduler registered under name. The lookup is
// only used by schedulers that project running-job completion times
// (backfill); passing nil is fine for the others.
func ByName(name string, a Allocator, lookup JobLookup) (Scheduler, bool) {
	f, ok := registry[name]
	if !ok {
		return nil, false
	}
	return f(a, lookup), true
}

// Names lists the registered scheduler names in sorted order.
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
