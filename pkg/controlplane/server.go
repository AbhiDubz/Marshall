// Package controlplane implements marshald: the gRPC control plane
// that owns the store, runs the scheduling loop, and dispatches jobs
// to node agents.
package controlplane

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/AbhiDubz/Marshall/pkg/metrics"
	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/store"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// FullStore is what the control plane needs from persistence.
type FullStore interface {
	store.Store
	store.PayloadStore
}

// Config tunes the control plane.
type Config struct {
	ScheduleEvery    time.Duration // scheduler loop period (default 2s)
	HeartbeatTimeout time.Duration // node considered dead after this (default 15s)
}

func (c *Config) defaults() {
	if c.ScheduleEvery == 0 {
		c.ScheduleEvery = 2 * time.Second
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 15 * time.Second
	}
}

// Server implements marshalpb.ControlPlaneServer.
type Server struct {
	marshalpb.UnimplementedControlPlaneServer

	st      FullStore
	sched   sched.Scheduler
	cfg     Config
	now     func() time.Time
	metrics *metrics.Registry // optional

	mu         sync.Mutex
	agentAddrs map[string]string // nodeID -> dial address
	conns      map[string]*grpc.ClientConn
	nextJobID  int64

	// dialAgent is swappable in tests (bufconn).
	dialAgent func(ctx context.Context, addr string) (marshalpb.AgentClient, error)
}

// New builds a Server. The scheduler is constructed by the caller so
// any registered policy can drive the real cluster too.
func New(st FullStore, scheduler sched.Scheduler, cfg Config) *Server {
	cfg.defaults()
	s := &Server{
		st:         st,
		sched:      scheduler,
		cfg:        cfg,
		now:        func() time.Time { return time.Now().UTC() },
		agentAddrs: make(map[string]string),
		conns:      make(map[string]*grpc.ClientConn),
	}
	s.dialAgent = s.grpcDial
	return s
}

// SetClock injects a clock for tests.
func (s *Server) SetClock(now func() time.Time) { s.now = now }

// SetMetrics attaches Prometheus instrumentation.
func (s *Server) SetMetrics(m *metrics.Registry) { s.metrics = m }

// SetAgentDialer injects an agent dialer for tests.
func (s *Server) SetAgentDialer(d func(ctx context.Context, addr string) (marshalpb.AgentClient, error)) {
	s.dialAgent = d
}

func (s *Server) grpcDial(_ context.Context, addr string) (marshalpb.AgentClient, error) {
	s.mu.Lock()
	conn, ok := s.conns[addr]
	s.mu.Unlock()
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.conns[addr] = conn
		s.mu.Unlock()
	}
	return marshalpb.NewAgentClient(conn), nil
}

// Token composes the dispatch idempotency token for a job attempt.
func Token(jobID string, attempt int) string { return jobID + "/" + strconv.Itoa(attempt) }

// ParseToken splits a token back into (jobID, attempt).
func ParseToken(token string) (string, int, error) {
	i := strings.LastIndexByte(token, '/')
	if i < 0 {
		return "", 0, fmt.Errorf("malformed token %q", token)
	}
	attempt, err := strconv.Atoi(token[i+1:])
	if err != nil {
		return "", 0, fmt.Errorf("malformed token %q: %v", token, err)
	}
	return token[:i], attempt, nil
}

// ---- client-facing RPCs ----

func (s *Server) SubmitJob(ctx context.Context, req *marshalpb.SubmitJobRequest) (*marshalpb.SubmitJobResponse, error) {
	spec := req.GetSpec()
	if spec.GetCmd() == "" {
		return nil, status.Error(codes.InvalidArgument, "cmd is required")
	}
	if spec.GetRequest().GetCpuMillis() <= 0 || spec.GetRequest().GetMemoryBytes() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "request.cpu_millis and request.memory_bytes must be positive")
	}
	now := s.now()
	s.mu.Lock()
	s.nextJobID++
	id := fmt.Sprintf("job-%d-%04d", now.UnixMilli(), s.nextJobID)
	s.mu.Unlock()

	job := types.Job{
		ID:       id,
		User:     spec.GetUser(),
		Priority: int(spec.GetPriority()),
		Request: types.ResourceSpec{
			CPUMillis:   spec.GetRequest().GetCpuMillis(),
			MemoryBytes: spec.GetRequest().GetMemoryBytes(),
			GRES:        gresFromPB(spec.GetRequest().GetGres()),
		},
		NodeCount:  max(int(spec.GetNodeCount()), 1),
		EstRuntime: time.Duration(spec.GetEstRuntimeMs()) * time.Millisecond,
		SubmitAt:   now,
		State:      types.Pending,
	}
	if err := s.st.CreateJob(ctx, job); err != nil {
		return nil, status.Errorf(codes.Internal, "create job: %v", err)
	}
	if err := s.st.PutPayload(ctx, id, spec.GetCmd()); err != nil {
		return nil, status.Errorf(codes.Internal, "store payload: %v", err)
	}
	return &marshalpb.SubmitJobResponse{JobId: id}, nil
}

func (s *Server) GetJob(ctx context.Context, req *marshalpb.GetJobRequest) (*marshalpb.JobInfo, error) {
	job, err := s.st.GetJob(ctx, req.GetJobId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return s.jobInfo(ctx, job)
}

func (s *Server) CancelJob(ctx context.Context, req *marshalpb.CancelJobRequest) (*marshalpb.CancelJobResponse, error) {
	id := req.GetJobId()
	job, err := s.st.GetJob(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	now := s.now()
	switch job.State {
	case types.Pending, types.Scheduled:
		if _, err := s.st.TransitionJob(ctx, id, job.State, types.Failed, now); err != nil {
			return nil, status.Errorf(codes.Aborted, "%v", err)
		}
	case types.Running:
		s.killOnAgents(ctx, job) // best effort
		if _, err := s.st.TransitionJob(ctx, id, types.Running, types.Failed, now); err != nil {
			return nil, status.Errorf(codes.Aborted, "%v", err)
		}
		s.releaseAllocation(ctx, job)
	default:
		return &marshalpb.CancelJobResponse{State: string(job.State)}, nil
	}
	return &marshalpb.CancelJobResponse{State: string(types.Failed)}, nil
}

func (s *Server) ListJobs(ctx context.Context, req *marshalpb.ListJobsRequest) (*marshalpb.ListJobsResponse, error) {
	states := make([]types.JobState, 0, len(req.GetStates()))
	for _, st := range req.GetStates() {
		states = append(states, types.JobState(st))
	}
	jobs, err := s.st.ListJobs(ctx, states...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	resp := &marshalpb.ListJobsResponse{}
	for _, j := range jobs {
		info, err := s.jobInfo(ctx, j)
		if err != nil {
			return nil, err
		}
		resp.Jobs = append(resp.Jobs, info)
	}
	return resp, nil
}

func (s *Server) ListNodes(ctx context.Context, _ *marshalpb.ListNodesRequest) (*marshalpb.ListNodesResponse, error) {
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	now := s.now()
	resp := &marshalpb.ListNodesResponse{}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, &marshalpb.NodeInfo{
			Id:                  n.ID,
			Capacity:            pbSpec(n.Capacity),
			Allocated:           pbSpec(n.Allocated),
			LastHeartbeatUnixMs: n.LastHeartbeat.UnixMilli(),
			Draining:            n.Draining,
			Healthy:             now.Sub(n.LastHeartbeat) < s.cfg.HeartbeatTimeout,
		})
	}
	return resp, nil
}

// ---- agent-facing RPCs ----

func (s *Server) RegisterNode(ctx context.Context, req *marshalpb.RegisterNodeRequest) (*marshalpb.RegisterNodeResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	n := types.Node{
		ID: id,
		Capacity: types.ResourceSpec{
			CPUMillis:   req.GetCapacity().GetCpuMillis(),
			MemoryBytes: req.GetCapacity().GetMemoryBytes(),
			GRES:        gresFromPB(req.GetCapacity().GetGres()),
		},
		LastHeartbeat: s.now(),
	}
	if err := s.st.UpsertNode(ctx, n); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.mu.Lock()
	s.agentAddrs[id] = req.GetAgentAddr()
	s.mu.Unlock()
	log.Printf("node %s registered from %s (cpu=%dm mem=%dB)", id, req.GetAgentAddr(),
		n.Capacity.CPUMillis, n.Capacity.MemoryBytes)
	return &marshalpb.RegisterNodeResponse{NodeId: id}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *marshalpb.HeartbeatRequest) (*marshalpb.HeartbeatResponse, error) {
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	var node *types.Node
	for i := range nodes {
		if nodes[i].ID == req.GetNodeId() {
			node = &nodes[i]
			break
		}
	}
	if node == nil {
		return nil, status.Errorf(codes.NotFound, "unknown node %s (register first)", req.GetNodeId())
	}
	node.LastHeartbeat = s.now()
	if err := s.st.UpsertNode(ctx, *node); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.mu.Lock()
	if addr := req.GetAgentAddr(); addr != "" {
		s.agentAddrs[req.GetNodeId()] = addr
	}
	s.mu.Unlock()

	// Fencing: any running token whose (job, attempt) no longer
	// matches the store's view must be killed.
	resp := &marshalpb.HeartbeatResponse{}
	for _, token := range req.GetRunningTokens() {
		jobID, attempt, err := ParseToken(token)
		if err != nil {
			resp.KillTokens = append(resp.KillTokens, token)
			continue
		}
		job, err := s.st.GetJob(ctx, jobID)
		if err != nil || job.State != types.Running || job.Attempt != attempt {
			resp.KillTokens = append(resp.KillTokens, token)
		}
	}
	return resp, nil
}

func (s *Server) ReportJob(ctx context.Context, req *marshalpb.ReportJobRequest) (*marshalpb.ReportJobResponse, error) {
	jobID, attempt, err := ParseToken(req.GetToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	job, err := s.st.GetJob(ctx, jobID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if job.State != types.Running || job.Attempt != attempt {
		// Stale attempt reporting after a requeue: ignore, fenced.
		log.Printf("ignoring stale report for %s (state=%s attempt=%d)", req.GetToken(), job.State, job.Attempt)
		return &marshalpb.ReportJobResponse{}, nil
	}
	to := types.Completed
	if !req.GetSuccess() {
		to = types.Failed
	}
	now := s.now()
	if _, err := s.st.TransitionJob(ctx, jobID, types.Running, to, now); err != nil {
		return nil, status.Errorf(codes.Aborted, "%v", err)
	}
	s.releaseAllocation(ctx, job)
	if s.metrics != nil {
		s.metrics.JobsFinished.WithLabelValues(string(to)).Inc()
	}
	log.Printf("job %s attempt %d finished: %s (exit=%d)", jobID, attempt, to, req.GetExitCode())
	return &marshalpb.ReportJobResponse{}, nil
}

// ---- helpers ----

func (s *Server) jobInfo(ctx context.Context, j types.Job) (*marshalpb.JobInfo, error) {
	cmd, _ := s.st.GetPayload(ctx, j.ID)
	info := &marshalpb.JobInfo{
		Id: j.ID,
		Spec: &marshalpb.JobSpec{
			User:         j.User,
			Priority:     int32(j.Priority),
			Request:      pbSpec(j.Request),
			NodeCount:    int32(j.NodeCount),
			EstRuntimeMs: j.EstRuntime.Milliseconds(),
			Cmd:          cmd,
		},
		State:          string(j.State),
		Attempt:        int32(j.Attempt),
		SubmitAtUnixMs: j.SubmitAt.UnixMilli(),
	}
	if alloc, ok := s.allocationFor(ctx, j); ok {
		info.NodeIds = alloc.NodeIDs
	}
	return info, nil
}

func (s *Server) allocationFor(ctx context.Context, j types.Job) (types.Allocation, bool) {
	allocs, err := s.st.Allocations(ctx)
	if err != nil {
		return types.Allocation{}, false
	}
	// Allocations are sorted by (job, attempt); the last one for the
	// job is the newest attempt.
	var out types.Allocation
	found := false
	for _, a := range allocs {
		if a.JobID == j.ID {
			out, found = a, true
		}
	}
	return out, found
}

func (s *Server) releaseAllocation(ctx context.Context, j types.Job) {
	alloc, ok := s.allocationFor(ctx, j)
	if !ok {
		return
	}
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return
	}
	for _, n := range nodes {
		for _, id := range alloc.NodeIDs {
			if n.ID == id {
				n.Allocated = n.Allocated.Sub(j.Request)
				_ = s.st.UpsertNode(ctx, n)
			}
		}
	}
}

func (s *Server) killOnAgents(ctx context.Context, j types.Job) {
	alloc, ok := s.allocationFor(ctx, j)
	if !ok {
		return
	}
	token := Token(j.ID, j.Attempt)
	for _, nodeID := range alloc.NodeIDs {
		if agent, err := s.agentFor(ctx, nodeID); err == nil {
			_, _ = agent.KillJob(ctx, &marshalpb.KillJobRequest{Token: token})
		}
	}
}

func (s *Server) agentFor(ctx context.Context, nodeID string) (marshalpb.AgentClient, error) {
	s.mu.Lock()
	addr, ok := s.agentAddrs[nodeID]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no agent address for node %s", nodeID)
	}
	return s.dialAgent(ctx, addr)
}

func pbSpec(r types.ResourceSpec) *marshalpb.ResourceSpec {
	out := &marshalpb.ResourceSpec{CpuMillis: r.CPUMillis, MemoryBytes: r.MemoryBytes}
	if len(r.GRES) > 0 {
		out.Gres = make(map[string]int64, len(r.GRES))
		for k, v := range r.GRES {
			out.Gres[k] = int64(v)
		}
	}
	return out
}

func gresFromPB(m map[string]int64) map[string]int {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = int(v)
	}
	return out
}
