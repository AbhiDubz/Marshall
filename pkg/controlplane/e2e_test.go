package controlplane

// End-to-end test of the M0.5 stack over in-process gRPC (bufconn):
// marshald server + 4 agents + MemStore. Verifies the acceptance
// criteria locally: a submitted command runs on a node and reaches
// COMPLETED; the node list shows 4 healthy nodes.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/AbhiDubz/Marshall/pkg/agent"
	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/store"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

type cluster struct {
	srv    *Server
	client marshalpb.ControlPlaneClient
	agents []*agent.Agent
}

func startCluster(t *testing.T, nodes int) *cluster {
	t.Helper()
	st := store.NewMemStore()
	srv := New(st, sched.NewFIFOScheduler(alloc.FirstFitAllocator{}), Config{
		ScheduleEvery: 50 * time.Millisecond, HeartbeatTimeout: 2 * time.Second,
	})

	// Control plane over bufconn.
	cpLis := bufconn.Listen(1 << 20)
	cpSrv := grpc.NewServer()
	marshalpb.RegisterControlPlaneServer(cpSrv, srv)
	go cpSrv.Serve(cpLis) //nolint:errcheck
	t.Cleanup(cpSrv.Stop)

	dial := func(lis *bufconn.Listener) *grpc.ClientConn {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	cpClient := marshalpb.NewControlPlaneClient(dial(cpLis))

	// Agents, each with its own bufconn listener.
	agentLis := make(map[string]*bufconn.Listener)
	c := &cluster{srv: srv, client: cpClient}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for i := 1; i <= nodes; i++ {
		id := fmt.Sprintf("node-%d", i)
		addr := "bufagent-" + id
		lis := bufconn.Listen(1 << 20)
		agentLis[addr] = lis

		a := agent.New(id, addr,
			types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30},
			agent.ExecRunner{}, cpClient)
		g := grpc.NewServer()
		marshalpb.RegisterAgentServer(g, a)
		go g.Serve(lis) //nolint:errcheck
		t.Cleanup(g.Stop)

		if err := a.Register(ctx); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		go a.HeartbeatLoop(ctx, 200*time.Millisecond)
		c.agents = append(c.agents, a)
	}

	srv.SetAgentDialer(func(_ context.Context, addr string) (marshalpb.AgentClient, error) {
		lis, ok := agentLis[addr]
		if !ok {
			return nil, fmt.Errorf("unknown agent addr %s", addr)
		}
		return marshalpb.NewAgentClient(dial(lis)), nil
	})

	go srv.RunLoop(ctx)
	return c
}

// e2eWait bounds how long an end-to-end assertion waits for a state
// transition. It is a failure ceiling, not an expected duration: a
// passing assertion returns as soon as the state matches, so a large
// value costs nothing and keeps the suite from flaking when `go test
// ./...` runs every package in parallel and starves these goroutines.
const e2eWait = 60 * time.Second

func waitForState(t *testing.T, c *cluster, jobID, want string, timeout time.Duration) *marshalpb.JobInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *marshalpb.JobInfo
	for time.Now().Before(deadline) {
		info, err := c.client.GetJob(context.Background(), &marshalpb.GetJobRequest{JobId: jobID})
		if err == nil {
			last = info
			if info.GetState() == want {
				return info
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %s (last: %v)", jobID, want, last)
	return nil
}

func TestEndToEndJobRunsAndCompletes(t *testing.T) {
	c := startCluster(t, 4)

	resp, err := c.client.SubmitJob(context.Background(), &marshalpb.SubmitJobRequest{
		Spec: &marshalpb.JobSpec{
			User: "alice", Priority: 5,
			Request:      &marshalpb.ResourceSpec{CpuMillis: 1000, MemoryBytes: 1 << 30},
			NodeCount:    1,
			EstRuntimeMs: 1000,
			Cmd:          "sleep 0.2",
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	info := waitForState(t, c, resp.GetJobId(), "COMPLETED", e2eWait)
	if len(info.GetNodeIds()) != 1 {
		t.Fatalf("job should have run on exactly one node: %v", info.GetNodeIds())
	}

	nodes, err := c.client.ListNodes(context.Background(), &marshalpb.ListNodesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes.GetNodes()) != 4 {
		t.Fatalf("want 4 nodes, got %d", len(nodes.GetNodes()))
	}
	for _, n := range nodes.GetNodes() {
		if !n.GetHealthy() {
			t.Fatalf("node %s not healthy", n.GetId())
		}
	}
}

func TestEndToEndFailedCommandReportsFailed(t *testing.T) {
	c := startCluster(t, 2)
	resp, err := c.client.SubmitJob(context.Background(), &marshalpb.SubmitJobRequest{
		Spec: &marshalpb.JobSpec{
			User:         "bob",
			Request:      &marshalpb.ResourceSpec{CpuMillis: 500, MemoryBytes: 1 << 30},
			EstRuntimeMs: 1000,
			Cmd:          "exit 3",
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitForState(t, c, resp.GetJobId(), "FAILED", e2eWait)
}

func TestEndToEndCancelPendingJob(t *testing.T) {
	c := startCluster(t, 1)
	// Fill the single node so the second job stays PENDING.
	blocker, err := c.client.SubmitJob(context.Background(), &marshalpb.SubmitJobRequest{
		Spec: &marshalpb.JobSpec{
			User:         "alice",
			Request:      &marshalpb.ResourceSpec{CpuMillis: 4000, MemoryBytes: 1 << 30},
			EstRuntimeMs: 60000,
			Cmd:          "sleep 120",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, c, blocker.GetJobId(), "RUNNING", e2eWait)

	waiting, err := c.client.SubmitJob(context.Background(), &marshalpb.SubmitJobRequest{
		Spec: &marshalpb.JobSpec{
			User:         "bob",
			Request:      &marshalpb.ResourceSpec{CpuMillis: 4000, MemoryBytes: 1 << 30},
			EstRuntimeMs: 60000,
			Cmd:          "sleep 120",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.client.CancelJob(context.Background(), &marshalpb.CancelJobRequest{JobId: waiting.GetJobId()}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitForState(t, c, waiting.GetJobId(), "FAILED", e2eWait)

	// The blocker is unaffected and still finishes... cancel it too to
	// end the test quickly, exercising the running-cancel path.

	// The blocker sleeps long enough that it is still RUNNING here even
	// on a loaded machine; a short sleep raced with the work above and
	// made this cancel fail with a state conflict under `go test ./...`.
	if _, err := c.client.CancelJob(context.Background(), &marshalpb.CancelJobRequest{JobId: blocker.GetJobId()}); err != nil {
		t.Fatalf("cancel running: %v", err)
	}
	waitForState(t, c, blocker.GetJobId(), "FAILED", e2eWait)
}

func TestTokenRoundTrip(t *testing.T) {
	token := Token("job-123", 4)
	id, attempt, err := ParseToken(token)
	if err != nil || id != "job-123" || attempt != 4 {
		t.Fatalf("round trip failed: %s %d %v", id, attempt, err)
	}
	if _, _, err := ParseToken("garbage"); err == nil {
		t.Fatal("malformed token must error")
	}
}
