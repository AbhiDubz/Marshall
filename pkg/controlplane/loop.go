package controlplane

import (
	"context"
	"log"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// RunLoop drives the scheduler until ctx is done.
func (s *Server) RunLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.ScheduleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.ScheduleOnce(ctx); err != nil {
				log.Printf("schedule cycle: %v", err)
			}
		}
	}
}

// ScheduleOnce runs one scheduling cycle: reap dead nodes, gather
// state, run the policy, dispatch. Exported so tests (and the M5
// dispatcher) can drive cycles deterministically.
func (s *Server) ScheduleOnce(ctx context.Context) error {
	now := s.now()
	if err := s.reapDeadNodes(ctx, now); err != nil {
		return err
	}

	pending, err := s.st.ListJobs(ctx, types.Pending)
	if err != nil {
		return err
	}
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return err
	}
	healthy := nodes[:0:0]
	for _, n := range nodes {
		if now.Sub(n.LastHeartbeat) < s.cfg.HeartbeatTimeout {
			healthy = append(healthy, n)
		}
	}

	runningJobs, err := s.st.ListJobs(ctx, types.Running)
	if err != nil {
		return err
	}
	var running []types.Allocation
	for _, j := range runningJobs {
		if a, ok := s.allocationFor(ctx, j); ok {
			running = append(running, a)
		}
	}

	if s.metrics != nil {
		s.metrics.ScheduleCycles.Inc()
		all, err := s.st.ListJobs(ctx)
		if err == nil {
			counts := make(map[types.JobState]int)
			for _, j := range all {
				counts[j.State]++
			}
			s.metrics.SnapshotJobs(counts)
		}
		s.metrics.SnapshotNodes(nodes, len(healthy))
	}

	if len(pending) == 0 || len(healthy) == 0 {
		return nil
	}
	allocs := s.sched.Schedule(now, pending, healthy, running)
	for _, a := range allocs {
		if err := s.dispatch(ctx, a, now); err != nil {
			log.Printf("dispatch %s: %v", a.JobID, err)
		}
	}
	return nil
}

// dispatch starts one allocation on its agents. This is the plain
// M0.5 path: SCHEDULED -> agent StartJob -> RUNNING, requeue on error.
// The failover-safe protocol lives in pkg/dispatch (stub #6) and
// replaces the middle of this function once implemented.
func (s *Server) dispatch(ctx context.Context, a types.Allocation, now time.Time) error {
	job, err := s.st.GetJob(ctx, a.JobID)
	if err != nil {
		return err
	}
	if _, err := s.st.TransitionJob(ctx, a.JobID, types.Pending, types.Scheduled, now); err != nil {
		return err
	}
	if err := s.st.RecordAllocation(ctx, a, job.Attempt); err != nil {
		return err
	}
	cmd, err := s.st.GetPayload(ctx, a.JobID)
	if err != nil {
		return err
	}
	token := Token(job.ID, job.Attempt)

	started := 0
	for _, nodeID := range a.NodeIDs {
		agent, err := s.agentFor(ctx, nodeID)
		if err == nil {
			_, err = agent.StartJob(ctx, &marshalpb.StartJobRequest{
				Token:   token,
				JobId:   job.ID,
				Attempt: int32(job.Attempt),
				Cmd:     cmd,
				Request: pbSpec(job.Request),
			})
		}
		if err != nil {
			log.Printf("start %s on %s failed: %v", token, nodeID, err)
			break
		}
		started++
	}
	if started < len(a.NodeIDs) {
		// All-or-nothing: kill what started, requeue.
		for _, nodeID := range a.NodeIDs[:started] {
			if agent, err := s.agentFor(ctx, nodeID); err == nil {
				_, _ = agent.KillJob(ctx, &marshalpb.KillJobRequest{Token: token})
			}
		}
		if s.metrics != nil {
			s.metrics.Dispatches.WithLabelValues("requeued").Inc()
		}
		_, err := s.st.TransitionJob(ctx, a.JobID, types.Scheduled, types.Pending, now)
		return err
	}

	if _, err := s.st.TransitionJob(ctx, a.JobID, types.Scheduled, types.Running, now); err != nil {
		return err
	}
	// Charge the nodes.
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		for _, id := range a.NodeIDs {
			if n.ID == id {
				n.Allocated = n.Allocated.Add(job.Request)
				if err := s.st.UpsertNode(ctx, n); err != nil {
					return err
				}
			}
		}
	}
	if s.metrics != nil {
		s.metrics.Dispatches.WithLabelValues("ok").Inc()
		s.metrics.ObserveWait(job.Priority, now.Sub(job.SubmitAt).Seconds())
	}
	log.Printf("job %s attempt %d running on %v", job.ID, job.Attempt, a.NodeIDs)
	return nil
}

// reapDeadNodes requeues RUNNING jobs whose nodes stopped
// heartbeating: RUNNING -> FAILED -> PENDING (Attempt+1), releasing
// the dead node's book-kept allocation.
func (s *Server) reapDeadNodes(ctx context.Context, now time.Time) error {
	nodes, err := s.st.ListNodes(ctx)
	if err != nil {
		return err
	}
	dead := make(map[string]bool)
	for _, n := range nodes {
		if now.Sub(n.LastHeartbeat) >= s.cfg.HeartbeatTimeout {
			dead[n.ID] = true
		}
	}
	if len(dead) == 0 {
		return nil
	}
	running, err := s.st.ListJobs(ctx, types.Running)
	if err != nil {
		return err
	}
	for _, j := range running {
		a, ok := s.allocationFor(ctx, j)
		if !ok {
			continue
		}
		hit := false
		for _, id := range a.NodeIDs {
			if dead[id] {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		log.Printf("node death: requeueing job %s (attempt %d)", j.ID, j.Attempt)
		if _, err := s.st.TransitionJob(ctx, j.ID, types.Running, types.Failed, now); err != nil {
			return err
		}
		if _, err := s.st.TransitionJob(ctx, j.ID, types.Failed, types.Pending, now); err != nil {
			return err
		}
		s.releaseAllocation(ctx, j)
	}
	return nil
}
