// marshal-agent runs on each worker node: registers with marshald,
// heartbeats, executes dispatched jobs, reports status.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AbhiDubz/Marshall/pkg/agent"
	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

func main() {
	var (
		nodeID    = flag.String("node-id", hostnameDefault(), "node identifier")
		listen    = flag.String("listen", ":7071", "agent gRPC listen address")
		advertise = flag.String("advertise", "", "address marshald dials back on (default: <hostname><listen-port>)")
		server    = flag.String("server", "localhost:7070", "marshald address")
		cpu       = flag.Int64("cpu-millis", int64(runtime.NumCPU())*1000, "advertised CPU capacity (millicores)")
		memBytes  = flag.Int64("memory-bytes", 8<<30, "advertised memory capacity")
		runner    = flag.String("runner", "exec", "job runner: exec | docker")
		image     = flag.String("docker-image", "alpine:3", "image for --runner=docker")
		hbEvery   = flag.Duration("heartbeat", 3*time.Second, "heartbeat interval")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *server, err)
	}
	defer conn.Close()
	cp := marshalpb.NewControlPlaneClient(conn)

	var r agent.Runner = agent.ExecRunner{}
	if *runner == "docker" {
		r = agent.DockerRunner{Image: *image}
	}

	addr := *advertise
	if addr == "" {
		host, _ := os.Hostname()
		_, port, err := net.SplitHostPort(*listen)
		if err != nil {
			log.Fatalf("bad --listen %q: %v", *listen, err)
		}
		addr = net.JoinHostPort(host, port)
	}

	a := agent.New(*nodeID, addr, types.ResourceSpec{CPUMillis: *cpu, MemoryBytes: *memBytes}, r, cp)

	// Register with retry: marshald may still be coming up.
	for {
		if err := a.Register(ctx); err == nil {
			break
		} else {
			log.Printf("register: %v (retrying)", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	go a.HeartbeatLoop(ctx, *hbEvery)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	marshalpb.RegisterAgentServer(g, a)
	log.Printf("marshal-agent %s listening on %s (advertise %s, runner=%s)", *nodeID, *listen, addr, *runner)

	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func hostnameDefault() string {
	h, err := os.Hostname()
	if err != nil {
		return "node-unknown"
	}
	return h
}
