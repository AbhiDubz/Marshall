// marshald is the control plane daemon: gRPC API, scheduler loop,
// Postgres-backed state.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/controlplane"
	"github.com/AbhiDubz/Marshall/pkg/metrics"
	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/store"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

func main() {
	var (
		listen        = flag.String("listen", ":7070", "gRPC listen address")
		metricsListen = flag.String("metrics-listen", ":9090", "Prometheus /metrics address (empty to disable)")
		dsn           = flag.String("dsn", "postgres://marshal:marshal@localhost:5433/marshal?sslmode=disable", "Postgres DSN")
		schedName     = flag.String("sched", "fifo", "scheduler policy")
		allocName     = flag.String("alloc", "firstfit", "allocator")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.OpenPG(ctx, *dsn)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	a, ok := alloc.ByName(*allocName)
	if !ok {
		log.Fatalf("unknown allocator %q (have %v)", *allocName, alloc.Names())
	}
	// Schedulers that project running-job completions resolve jobs
	// straight from the store.
	lookup := sched.JobLookup(func(id string) (types.Job, bool) {
		j, err := st.GetJob(ctx, id)
		return j, err == nil
	})
	policy, ok := sched.ByName(*schedName, a, lookup)
	if !ok {
		log.Fatalf("unknown scheduler %q (have %v)", *schedName, sched.Names())
	}

	srv := controlplane.New(st, policy, controlplane.Config{})

	if *metricsListen != "" {
		reg := metrics.New()
		srv.SetMetrics(reg)
		mux := http.NewServeMux()
		mux.Handle("/metrics", reg.Handler())
		go func() {
			log.Printf("metrics on %s/metrics", *metricsListen)
			if err := http.ListenAndServe(*metricsListen, mux); err != nil {
				log.Printf("metrics server: %v", err)
			}
		}()
	}

	// Leader election: only the advisory-lock holder runs the
	// scheduling loop (see ADR-0005). The API serves regardless —
	// reads are safe from any replica.
	go func() {
		lead, err := store.WaitLead(ctx, st.Pool(), store.LeaderKey, 2*time.Second)
		if err != nil {
			if ctx.Err() == nil {
				log.Fatalf("leader election: %v", err)
			}
			return
		}
		defer lead.Release()
		log.Printf("acquired leadership; starting scheduler loop")
		srv.RunLoop(ctx)
	}()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	marshalpb.RegisterControlPlaneServer(g, srv)
	log.Printf("marshald listening on %s (sched=%s alloc=%s)", *listen, *schedName, *allocName)

	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
