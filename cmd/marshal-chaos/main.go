// marshal-chaos runs seeded fault-injection campaigns against the
// simulated control loop and checks invariants continuously.
//
//	marshal-chaos --seeds 1000          # campaign over seeds 1..1000
//	marshal-chaos --seed 137 -v         # replay one seed exactly
//
// Any failing seed replays byte-for-byte with --seed N: the entire
// run is a pure function of (config, seed).
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/chaos"
)

func main() {
	var (
		seeds      = flag.Int("seeds", 0, "run a campaign over seeds 1..N")
		seed       = flag.Int64("seed", 0, "replay a single seed")
		nodes      = flag.Int("nodes", 6, "cluster size")
		jobs       = flag.Int("jobs", 30, "workload size")
		horizon    = flag.Int("horizon", 14400, "ticks (500ms each)")
		schedName  = flag.String("sched", "fifo", "scheduler policy")
		allocName  = flag.String("alloc", "firstfit", "allocator")
		startBound = flag.Duration("start-bound", 60*time.Minute, "starvation bound")
		verbose    = flag.Bool("v", false, "print the fault plan on single-seed replay")
	)
	flag.Parse()

	cfg := chaos.Config{
		Nodes: *nodes, Jobs: *jobs, Horizon: *horizon,
		Scheduler: *schedName, Allocator: *allocName, StartBound: *startBound,
	}

	switch {
	case *seed != 0:
		r := chaos.RunSeed(cfg, *seed)
		if *verbose {
			fmt.Println(chaos.DescribeSeed(cfg, *seed))
		}
		fmt.Println(r)
		if r.Violation != nil {
			os.Exit(1)
		}

	case *seeds > 0:
		start := time.Now()
		_, failures := chaos.Campaign(cfg, *seeds, func(r chaos.Result) {
			if r.Violation != nil {
				fmt.Println(r)
			}
		})
		fmt.Printf("campaign: %d seeds, %d failures, %s\n", *seeds, len(failures), time.Since(start).Round(time.Millisecond))
		if len(failures) > 0 {
			fmt.Println("replay failures exactly with: marshal-chaos --seed N")
			os.Exit(1)
		}

	default:
		fmt.Fprintln(os.Stderr, "marshal-chaos: pass --seeds N or --seed N")
		os.Exit(2)
	}
}
