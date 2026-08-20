// Package agent implements marshal-agent: registers with marshald,
// heartbeats, executes jobs via a Runner, and reports terminal status.
//
// The agent keeps an idempotency table keyed by dispatch token
// (jobID/attempt). StartJob with a known token never launches a second
// execution — the node-local half of the exactly-once protocol.
package agent

import (
	"context"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Runner executes a job command and blocks until it exits.
type Runner interface {
	Run(ctx context.Context, token, cmd string) (exitCode int, err error)
}

// ExecRunner runs commands with `sh -c` in the agent's own
// environment — the default inside a container (the agent's pod *is*
// the job sandbox on k8s).
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, _ string, cmd string) (int, error) {
	c := exec.CommandContext(ctx, "sh", "-c", cmd)
	err := c.Run()
	if err == nil {
		return 0, nil
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), nil
	}
	return -1, err
}

// DockerRunner runs each job in a fresh container.
type DockerRunner struct {
	Image string // default alpine:3
}

func (r DockerRunner) Run(ctx context.Context, token, cmd string) (int, error) {
	image := r.Image
	if image == "" {
		image = "alpine:3"
	}
	c := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--name", "marshal-"+sanitize(token), image, "sh", "-c", cmd)
	err := c.Run()
	if err == nil {
		return 0, nil
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), nil
	}
	return -1, err
}

func sanitize(s string) string {
	out := []byte(s)
	for i, c := range out {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			out[i] = '-'
		}
	}
	return string(out)
}

type execution struct {
	cancel   context.CancelFunc
	finished bool
	success  bool
}

// Agent implements marshalpb.AgentServer.
type Agent struct {
	marshalpb.UnimplementedAgentServer

	NodeID   string
	Addr     string // advertised dial-back address
	Capacity types.ResourceSpec
	Runner   Runner

	cp marshalpb.ControlPlaneClient

	mu     sync.Mutex
	tokens map[string]*execution
}

// New builds an agent bound to a control-plane client.
func New(nodeID, addr string, capacity types.ResourceSpec, runner Runner, cp marshalpb.ControlPlaneClient) *Agent {
	return &Agent{
		NodeID: nodeID, Addr: addr, Capacity: capacity, Runner: runner,
		cp: cp, tokens: make(map[string]*execution),
	}
}

// Register announces the node to the control plane.
func (a *Agent) Register(ctx context.Context) error {
	_, err := a.cp.RegisterNode(ctx, &marshalpb.RegisterNodeRequest{
		NodeId:    a.NodeID,
		AgentAddr: a.Addr,
		Capacity: &marshalpb.ResourceSpec{
			CpuMillis:   a.Capacity.CPUMillis,
			MemoryBytes: a.Capacity.MemoryBytes,
		},
	})
	return err
}

// HeartbeatLoop sends heartbeats every interval until ctx is done,
// executing any kill directives the control plane returns.
func (a *Agent) HeartbeatLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.heartbeatOnce(ctx)
		}
	}
}

func (a *Agent) heartbeatOnce(ctx context.Context) {
	a.mu.Lock()
	var runningTokens []string
	for token, ex := range a.tokens {
		if !ex.finished {
			runningTokens = append(runningTokens, token)
		}
	}
	a.mu.Unlock()

	resp, err := a.cp.Heartbeat(ctx, &marshalpb.HeartbeatRequest{
		NodeId: a.NodeID, AgentAddr: a.Addr, RunningTokens: runningTokens,
	})
	if err != nil {
		log.Printf("heartbeat: %v", err)
		return
	}
	for _, token := range resp.GetKillTokens() {
		a.kill(token)
	}
}

// StartJob implements the idempotent dispatch entry point.
func (a *Agent) StartJob(ctx context.Context, req *marshalpb.StartJobRequest) (*marshalpb.StartJobResponse, error) {
	a.mu.Lock()
	if _, known := a.tokens[req.GetToken()]; known {
		a.mu.Unlock()
		return &marshalpb.StartJobResponse{AlreadyKnown: true}, nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	ex := &execution{cancel: cancel}
	a.tokens[req.GetToken()] = ex
	a.mu.Unlock()

	go a.run(runCtx, req.GetToken(), req.GetCmd(), ex)
	return &marshalpb.StartJobResponse{AlreadyKnown: false}, nil
}

func (a *Agent) run(ctx context.Context, token, cmd string, ex *execution) {
	exit, err := a.Runner.Run(ctx, token, cmd)
	success := err == nil && exit == 0
	a.mu.Lock()
	ex.finished, ex.success = true, success
	a.mu.Unlock()

	reportCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, rerr := a.cp.ReportJob(reportCtx, &marshalpb.ReportJobRequest{
		Token: token, Success: success, ExitCode: int32(exit),
	}); rerr != nil {
		log.Printf("report %s: %v", token, rerr)
	}
}

func (a *Agent) ProbeToken(_ context.Context, req *marshalpb.ProbeTokenRequest) (*marshalpb.ProbeTokenResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ex, known := a.tokens[req.GetToken()]
	resp := &marshalpb.ProbeTokenResponse{Known: known}
	if known {
		resp.Finished, resp.Success = ex.finished, ex.success
	}
	return resp, nil
}

func (a *Agent) KillJob(_ context.Context, req *marshalpb.KillJobRequest) (*marshalpb.KillJobResponse, error) {
	return &marshalpb.KillJobResponse{WasRunning: a.kill(req.GetToken())}, nil
}

func (a *Agent) kill(token string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	ex, ok := a.tokens[token]
	if !ok || ex.finished {
		return false
	}
	ex.cancel()
	return true
}
