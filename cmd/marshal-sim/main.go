// marshal-sim replays workload traces through a scheduler + allocator
// and reports utilization, wait times by priority, and makespan.
//
// Modes:
//
//	marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42
//	marshal-sim --compare [--trace traces/uniform.json,traces/bursty.json] [--chart docs/compare.svg]
//	marshal-sim --gen --kind uniform --seed 42 --jobs 200 --out traces/uniform.json
//
// Determinism: the same trace, scheduler, and allocator produce
// byte-identical output on every run. Policies that are still stubbed
// (panic: not implemented) are reported as NOT IMPL in --compare and
// as an error in single-run mode — never as fabricated numbers.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/sim"
)

func main() {
	var (
		tracePath = flag.String("trace", "", "trace file (comma-separated list in --compare mode)")
		schedName = flag.String("sched", "fifo", "scheduler: "+strings.Join(sched.Names(), ", "))
		allocName = flag.String("alloc", "firstfit", "allocator: "+strings.Join(alloc.Names(), ", "))
		seed      = flag.Int64("seed", 42, "seed for generation (and any future stochastic sim features)")
		compare   = flag.Bool("compare", false, "run every scheduler x allocator over the trace set")
		chart     = flag.String("chart", "", "in --compare mode, write an SVG utilization chart here")
		gen       = flag.Bool("gen", false, "generate a trace instead of running one")
		kind      = flag.String("kind", "uniform", "trace kind for --gen: uniform, bimodal, bursty")
		jobs      = flag.Int("jobs", 200, "job count for --gen")
		out       = flag.String("out", "", "output path for --gen")
	)
	flag.Parse()

	switch {
	case *gen:
		if *out == "" {
			fatal("--gen requires --out")
		}
		tr, err := sim.Generate(*kind, *seed, *jobs)
		if err != nil {
			fatal("%v", err)
		}
		if err := tr.Save(*out); err != nil {
			fatal("%v", err)
		}
		fmt.Printf("wrote %s: kind=%s seed=%d jobs=%d\n", *out, *kind, *seed, len(tr.Jobs))

	case *compare:
		paths := defaultTraces(*tracePath)
		rows := runMatrix(paths)
		fmt.Print(sim.CompareTable(rows))
		if *chart != "" {
			if err := os.WriteFile(*chart, []byte(sim.CompareChartSVG(rows)), 0o644); err != nil {
				fatal("%v", err)
			}
			fmt.Printf("chart written to %s\n", *chart)
		}

	default:
		if *tracePath == "" {
			fatal("--trace is required (or use --gen / --compare)")
		}
		res, err := runOne(*tracePath, *schedName, *allocName)
		if err != nil {
			fatal("%v", err)
		}
		fmt.Print(res.String())
	}
}

func defaultTraces(flagVal string) []string {
	if flagVal != "" {
		return strings.Split(flagVal, ",")
	}
	var paths []string
	for _, name := range []string{"uniform", "bimodal", "bursty"} {
		p := "traces/" + name + ".json"
		if _, err := os.Stat(p); err == nil {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		fatal("no traces found under traces/; pass --trace")
	}
	return paths
}

func runOne(path, schedName, allocName string) (res *sim.Result, err error) {
	tr, err := sim.Load(path)
	if err != nil {
		return nil, err
	}
	a, ok := alloc.ByName(allocName)
	if !ok {
		return nil, fmt.Errorf("unknown allocator %q (have: %s)", allocName, strings.Join(alloc.Names(), ", "))
	}
	e := sim.NewEngine(tr)
	s, ok := sched.ByName(schedName, a, e.Lookup)
	if !ok {
		return nil, fmt.Errorf("unknown scheduler %q (have: %s)", schedName, strings.Join(sched.Names(), ", "))
	}
	e.SetScheduler(s)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s/%s is not implemented yet (stub): %v", schedName, allocName, r)
		}
	}()
	res, err = e.Run()
	if err != nil {
		return nil, err
	}
	res.Allocator = allocName
	return res, nil
}

func runMatrix(paths []string) []sim.CompareRow {
	var rows []sim.CompareRow
	for _, p := range paths {
		tr, err := sim.Load(p)
		if err != nil {
			fatal("%v", err)
		}
		for _, sn := range sched.Names() {
			for _, an := range alloc.Names() {
				res, err := runOne(p, sn, an)
				if err != nil {
					res = nil // stubbed or failed: shown as NOT IMPL, never invented
				}
				rows = append(rows, sim.CompareRow{
					Trace: tr.Name, Scheduler: sn, Allocator: an, Result: res,
				})
			}
		}
	}
	return rows
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "marshal-sim: "+format+"\n", args...)
	os.Exit(1)
}
