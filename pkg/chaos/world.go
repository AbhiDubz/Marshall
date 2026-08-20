package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/sched"
	"github.com/AbhiDubz/Marshall/pkg/store"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

// World simulates the full system: a controller (scheduler + failure
// detector + fencing, state in a store.MemStore so every transition is
// state-machine checked), N agents executing jobs, and a lossy network
// between them. Time advances in fixed ticks; everything is a pure
// function of (config, seed).
type World struct {
	cfg  Config
	rng  *rand.Rand // heartbeat drop decisions only
	plan Plan

	tick int
	now  time.Time

	agents  map[string]*simAgent
	ctl     *controller
	inbox   []message // network in flight, sorted deterministically
	checker *Checker

	jobs map[string]*chaosJob
}

type chaosJob struct {
	job     types.Job
	trueRun time.Duration
	submit  int // tick
}

type simAgent struct {
	id       string
	capacity types.ResourceSpec

	alive       bool
	paused      bool
	partitioned bool
	reviveTick  int
	skew        float64

	execs map[string]*execution // token -> execution

	// completions the agent could not deliver (partition); retried on
	// heal.
	pendingReports []message

	nextHB float64 // tick index (fractional under skew) of next heartbeat
}

type execution struct {
	jobID    string
	attempt  int
	req      types.ResourceSpec
	progress time.Duration
	needed   time.Duration
	fenced   bool
	done     bool
	primary  bool // only the primary shard reports completion
}

type message struct {
	deliverAt int
	sentAt    int
	kind      string // "heartbeat", "report"
	node      string
	tokens    []string // heartbeat: running tokens
	token     string   // report
	seq       int
}

type nodeView struct {
	lastSeen  int // tick of last heartbeat
	state     string // "alive", "dead", "readmitting"
	allocated types.ResourceSpec

	// fencing holds tokens whose death on this node is not yet
	// confirmed (their job was requeued while the node stayed alive).
	// A node with unconfirmed fences takes no new work — the
	// "completing" state that prevents double-booking a node that
	// still runs a zombie shard. Values are the tick fencing began.
	fencing map[string]int
}

type controller struct {
	st        *store.MemStore
	scheduler sched.Scheduler
	views     map[string]*nodeView
	allocs    map[string]types.Allocation // jobID -> current placement
	killQueue map[string][]string         // node -> tokens to kill on next contact
	hbTimeout int                         // ticks

	// missing counts consecutive heartbeats from a node that failed
	// to list a token the controller expects to be running there —
	// the reconciliation that catches executions lost to a node that
	// died and revived faster than the failure detector.
	missing map[string]map[string]int // node -> token -> misses

	// placedAt records the tick each token was dispatched. A
	// heartbeat sent at or before that tick cannot know about the
	// placement and must not count as evidence the execution is gone.
	placedAt map[string]int
}

// Config sizes one chaos run.
type Config struct {
	Nodes     int
	Jobs      int
	Horizon   int // ticks
	HealBy    int // ticks; all faults end before this
	Scheduler string
	Allocator string
	StartBound time.Duration // max queue wait before the starvation invariant fires
}

func (c *Config) defaults() {
	if c.Nodes == 0 {
		c.Nodes = 6
	}
	if c.Jobs == 0 {
		c.Jobs = 30
	}
	if c.Horizon == 0 {
		c.Horizon = 14400 // 2h at 500ms
	}
	if c.HealBy == 0 {
		c.HealBy = c.Horizon / 3
	}
	if c.Scheduler == "" {
		c.Scheduler = "fifo"
	}
	if c.Allocator == "" {
		c.Allocator = "firstfit"
	}
	if c.StartBound == 0 {
		c.StartBound = 60 * time.Minute
	}
}

const hbPeriodTicks = 6      // heartbeat every 3s
const hbTimeoutTicks = 30    // dead after 15s silent

var chaosEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// NewWorld builds the world for one seed.
func NewWorld(cfg Config, seed int64, scheduler sched.Scheduler) (*World, error) {
	cfg.defaults()
	planRng := rand.New(rand.NewSource(seed))
	w := &World{
		cfg:    cfg,
		rng:    rand.New(rand.NewSource(seed ^ 0x5eed)),
		agents: make(map[string]*simAgent),
		jobs:   make(map[string]*chaosJob),
		now:    chaosEpoch,
	}

	var nodeIDs []string
	for i := 1; i <= cfg.Nodes; i++ {
		id := fmt.Sprintf("node-%02d", i)
		nodeIDs = append(nodeIDs, id)
		w.agents[id] = &simAgent{
			id:       id,
			capacity: types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30},
			alive:    true,
			skew:     1.0,
			execs:    make(map[string]*execution),
		}
	}
	w.plan = GeneratePlan(planRng, nodeIDs, cfg.HealBy)

	// Workload: single-node jobs plus a few 2-node gangs, submitted in
	// the first quarter of the horizon.
	st := store.NewMemStore()
	for i := 1; i <= cfg.Jobs; i++ {
		submitTick := planRng.Intn(cfg.Horizon / 4)
		nodeCount := 1
		if i%10 == 0 {
			nodeCount = 2
		}
		run := time.Duration(30+planRng.Intn(150)) * time.Second
		j := types.Job{
			ID:       fmt.Sprintf("job-%03d", i),
			User:     "chaos",
			Priority: planRng.Intn(3) * 4,
			Request: types.ResourceSpec{
				CPUMillis:   []int64{1000, 2000, 4000}[planRng.Intn(3)],
				MemoryBytes: int64(1+planRng.Intn(4)) << 30,
			},
			NodeCount:  nodeCount,
			EstRuntime: run, // estimates exact here; estimate error is sim's concern
			SubmitAt:   chaosEpoch.Add(time.Duration(submitTick) * Tick),
			State:      types.Pending,
		}
		w.jobs[j.ID] = &chaosJob{job: j, trueRun: run, submit: submitTick}
	}

	w.ctl = &controller{
		st:        st,
		scheduler: scheduler,
		views:     make(map[string]*nodeView),
		allocs:    make(map[string]types.Allocation),
		killQueue: make(map[string][]string),
		hbTimeout: hbTimeoutTicks,
		missing:   make(map[string]map[string]int),
		placedAt:  make(map[string]int),
	}
	for _, id := range nodeIDs {
		w.ctl.views[id] = &nodeView{lastSeen: 0, state: "alive", fencing: make(map[string]int)}
	}
	w.checker = NewChecker(w)
	return w, nil
}

func (w *World) agentIDs() []string {
	ids := make([]string, 0, len(w.agents))
	for id := range w.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Step advances one tick. Order within a tick is fixed:
// faults -> agents act -> network delivers -> controller reacts ->
// scheduler places -> invariants checked.
func (w *World) Step() error {
	w.now = chaosEpoch.Add(time.Duration(w.tick) * Tick)

	w.applyFaults()
	w.agentsAct()
	delivered := w.deliver()
	if err := w.ctl.process(w, delivered); err != nil {
		return err
	}
	if err := w.ctl.detectFailures(w); err != nil {
		return err
	}
	if err := w.ctl.schedule(w); err != nil {
		return err
	}
	if err := w.checker.Check(w); err != nil {
		return err
	}
	w.tick++
	return nil
}

func (w *World) applyFaults() {
	for _, f := range w.plan.Faults {
		a := w.agents[f.Node]
		switch f.Kind {
		case FaultClockSkew:
			if w.tick == 0 {
				a.skew = f.SkewFactor
			}
		case FaultKill:
			if w.tick == f.StartTick {
				a.alive = false
				a.execs = make(map[string]*execution) // process gone
				a.pendingReports = nil
			}
			if w.tick == f.EndTick {
				a.alive = true
				a.nextHB = float64(w.tick)
			}
		case FaultPause:
			if w.tick == f.StartTick && a.alive {
				a.paused = true
			}
			if w.tick == f.EndTick {
				a.paused = false
			}
		case FaultPartition:
			if w.tick == f.StartTick {
				a.partitioned = true
			}
			if w.tick == f.EndTick {
				a.partitioned = false
			}
		}
	}
}

func (w *World) send(m message) {
	m.seq = len(w.inbox)*10000 + w.tick // deterministic tiebreak
	m.sentAt = w.tick
	w.inbox = append(w.inbox, m)
}

// agentsAct: run executions forward, emit heartbeats and completion
// reports subject to faults.
func (w *World) agentsAct() {
	for _, id := range w.agentIDs() {
		a := w.agents[id]
		if !a.alive || a.paused {
			continue
		}

		// Progress executions.
		var tokens []string
		for token := range a.execs {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)
		for _, token := range tokens {
			ex := a.execs[token]
			if ex.done {
				continue
			}
			ex.progress += Tick
			if ex.progress >= ex.needed {
				ex.done = true
				if ex.primary && !ex.fenced {
					rep := message{kind: "report", node: id, token: token, deliverAt: w.tick + 1}
					if a.partitioned {
						a.pendingReports = append(a.pendingReports, rep)
					} else {
						w.send(rep)
					}
				}
			}
		}

		// Heartbeats on the (skewed) period.
		if float64(w.tick) >= a.nextHB {
			a.nextHB += hbPeriodTicks / a.skew
			if a.partitioned {
				continue // dropped silently
			}
			drop, delay := w.hbFaults(id)
			if drop {
				continue
			}
			var running []string
			for _, token := range sortedTokens(a.execs) {
				if !a.execs[token].done {
					running = append(running, token)
				}
			}
			w.send(message{kind: "heartbeat", node: id, tokens: running, deliverAt: w.tick + 1 + delay})
		}

		// Retry queued reports once the partition heals.
		if !a.partitioned && len(a.pendingReports) > 0 {
			for _, rep := range a.pendingReports {
				rep.deliverAt = w.tick + 1
				w.send(rep)
			}
			a.pendingReports = nil
		}
	}
}

func sortedTokens(m map[string]*execution) []string {
	out := make([]string, 0, len(m))
	for t := range m {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (w *World) hbFaults(node string) (drop bool, delay int) {
	for _, f := range w.plan.Faults {
		if f.Node != node || w.tick < f.StartTick || w.tick >= f.EndTick {
			continue
		}
		switch f.Kind {
		case FaultHBDrop:
			if w.rng.Float64() < f.DropP {
				drop = true
			}
		case FaultHBDelay:
			delay += f.DelayTicks
		}
	}
	return drop, delay
}

// deliver returns the messages due this tick, in deterministic order.
// A partition is bidirectional and cuts in-flight traffic too: due
// messages from a currently-partitioned sender are dropped (heartbeats)
// or returned to the agent for retry on heal (reports).
func (w *World) deliver() []message {
	var due, rest []message
	for _, m := range w.inbox {
		if m.deliverAt > w.tick {
			rest = append(rest, m)
			continue
		}
		if a := w.agents[m.node]; a.partitioned {
			if m.kind == "report" {
				a.pendingReports = append(a.pendingReports, m)
			}
			continue
		}
		due = append(due, m)
	}
	w.inbox = rest
	sort.SliceStable(due, func(i, k int) bool {
		if due[i].node != due[k].node {
			return due[i].node < due[k].node
		}
		if due[i].kind != due[k].kind {
			return due[i].kind < due[k].kind
		}
		return due[i].seq < due[k].seq
	})
	return due
}

// ---- controller ----

func (c *controller) process(w *World, msgs []message) error {
	ctx := context.Background()
	for _, m := range msgs {
		switch m.kind {
		case "heartbeat":
			v := c.views[m.node]
			v.lastSeen = w.tick
			have := make(map[string]bool, len(m.tokens))
			for _, tkn := range m.tokens {
				have[tkn] = true
			}

			// Fencing: tokens whose (job, attempt) is no longer
			// RUNNING at that attempt must die.
			var kills []string
			killed := make(map[string]bool)
			for _, token := range m.tokens {
				jobID, attempt, err := parseToken(token)
				if err != nil {
					kills = append(kills, token)
					continue
				}
				j, err := c.st.GetJob(ctx, jobID)
				if err != nil || j.State != types.Running || j.Attempt != attempt {
					kills = append(kills, token)
				}
			}
			// The heartbeat response applies kills synchronously.
			agent := w.agents[m.node]
			for _, token := range kills {
				killed[token] = true
				if ex, ok := agent.execs[token]; ok {
					ex.fenced = true
					delete(agent.execs, token)
				}
			}

			// Resolve fences: a fenced token is confirmed gone once it
			// was just killed, or once a heartbeat *sent after fencing
			// began* no longer lists it (the shard finished or never
			// existed — either way it holds nothing).
			for token, since := range v.fencing {
				if killed[token] || (m.sentAt > since && !have[token]) {
					delete(v.fencing, token)
				}
			}

			switch v.state {
			case "dead":
				// Back from the dead: readmit only once clean.
				if len(m.tokens) == 0 || allKilled(m.tokens, kills) {
					v.state = "alive"
				} else {
					v.state = "readmitting"
				}
			case "readmitting":
				if len(m.tokens) == 0 || allKilled(m.tokens, kills) {
					v.state = "alive"
				}
			}

			// Reconciliation: a token the controller expects on this
			// node but the node stopped reporting means the execution
			// is gone (agent restarted faster than the failure
			// detector noticed). Two consecutive silent heartbeats —
			// grace for a completion report in flight — requeue the
			// job.
			if c.missing[m.node] == nil {
				c.missing[m.node] = make(map[string]int)
			}
			for _, jobID := range sortedAllocJobs(c.allocs) {
				a := c.allocs[jobID]
				onNode := false
				for _, nid := range a.NodeIDs {
					if nid == m.node {
						onNode = true
						break
					}
				}
				if !onNode {
					continue
				}
				j, err := c.st.GetJob(ctx, jobID)
				if err != nil || j.State != types.Running {
					continue
				}
				token := fmt.Sprintf("%s/%d", jobID, j.Attempt)
				if have[token] {
					c.missing[m.node][token] = 0
					continue
				}
				if m.sentAt <= c.placedAt[token] {
					continue // stale heartbeat: predates the placement
				}
				c.missing[m.node][token]++
				if c.missing[m.node][token] < 2 {
					continue
				}
				delete(c.missing[m.node], token)
				if err := c.requeue(w, jobID, j, m.node); err != nil {
					return fmt.Errorf("tick %d reconcile %s: %w", w.tick, jobID, err)
				}
			}

		case "report":
			jobID, attempt, err := parseToken(m.token)
			if err != nil {
				continue
			}
			j, err := c.st.GetJob(ctx, jobID)
			if err != nil {
				continue
			}
			if j.State != types.Running || j.Attempt != attempt {
				continue // stale attempt: fenced out, ignore
			}
			if _, err := c.st.TransitionJob(ctx, jobID, types.Running, types.Completed, w.now); err != nil {
				return fmt.Errorf("tick %d: accept completion %s: %w", w.tick, m.token, err)
			}
			w.checker.acceptedCompletion(jobID)
			c.release(jobID, j.Request)
		}
	}
	return nil
}

func allKilled(tokens, kills []string) bool {
	killed := make(map[string]bool, len(kills))
	for _, k := range kills {
		killed[k] = true
	}
	for _, t := range tokens {
		if !killed[t] {
			return false
		}
	}
	return true
}

// requeue moves a RUNNING job back to PENDING (Attempt+1), releases
// its books, and fences the old attempt's shards: every alive node of
// the old allocation (other than confirmedGone, whose absence
// triggered the requeue) is held out of scheduling until it confirms
// the shard is dead.
func (c *controller) requeue(w *World, jobID string, j types.Job, confirmedGone string) error {
	if _, err := c.st.TransitionJob(context.Background(), jobID, types.Running, types.Failed, w.now); err != nil {
		return err
	}
	if _, err := c.st.TransitionJob(context.Background(), jobID, types.Failed, types.Pending, w.now); err != nil {
		return err
	}
	token := fmt.Sprintf("%s/%d", jobID, j.Attempt)
	if a, ok := c.allocs[jobID]; ok {
		for _, nid := range a.NodeIDs {
			if nid == confirmedGone {
				continue
			}
			if v := c.views[nid]; v.state == "alive" {
				v.fencing[token] = w.tick
			}
		}
	}
	c.release(jobID, j.Request)
	w.checker.jobRequeued(jobID, w.tick)
	return nil
}

func (c *controller) release(jobID string, req types.ResourceSpec) {
	a, ok := c.allocs[jobID]
	if !ok {
		return
	}
	for _, nid := range a.NodeIDs {
		v := c.views[nid]
		v.allocated = v.allocated.Sub(req)
	}
	delete(c.allocs, jobID)
}

func (c *controller) detectFailures(w *World) error {
	ctx := context.Background()
	for _, id := range w.agentIDs() {
		v := c.views[id]
		if v.state != "alive" {
			continue
		}
		if w.tick-v.lastSeen < c.hbTimeout {
			continue
		}
		v.state = "dead"

		// Requeue every RUNNING job placed (partly) on the node.
		running, err := c.st.ListJobs(ctx, types.Running)
		if err != nil {
			return err
		}
		for _, j := range running {
			a, ok := c.allocs[j.ID]
			if !ok {
				continue
			}
			onNode := false
			for _, nid := range a.NodeIDs {
				if nid == id {
					onNode = true
					break
				}
			}
			if !onNode {
				continue
			}
			if err := c.requeue(w, j.ID, j, id); err != nil {
				return fmt.Errorf("tick %d requeue %s: %w", w.tick, j.ID, err)
			}
		}
		// Zero the dead node's book AFTER releases so surviving
		// nodes' books stay exact and this one carries no residue.
		v.allocated = types.ResourceSpec{}
	}
	return nil
}

func (c *controller) schedule(w *World) error {
	ctx := context.Background()

	// Submit jobs whose time has come.
	for _, id := range sortedJobIDs(w.jobs) {
		cj := w.jobs[id]
		if cj.submit == w.tick {
			if err := c.st.CreateJob(ctx, cj.job); err != nil {
				return err
			}
		}
	}

	pending, err := c.st.ListJobs(ctx, types.Pending)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	var nodes []types.Node
	for _, id := range w.agentIDs() {
		v := c.views[id]
		if v.state != "alive" || len(v.fencing) > 0 {
			continue // dead, readmitting, or completing (unconfirmed fences)
		}
		nodes = append(nodes, types.Node{
			ID:            id,
			Capacity:      w.agents[id].capacity.Clone(),
			Allocated:     v.allocated.Clone(),
			LastHeartbeat: chaosEpoch.Add(time.Duration(v.lastSeen) * Tick),
		})
	}
	var running []types.Allocation
	for _, id := range sortedAllocJobs(c.allocs) {
		running = append(running, c.allocs[id].Clone())
	}

	allocs := c.scheduler.Schedule(w.now, pending, nodes, running)
	for _, a := range allocs {
		j, err := c.st.GetJob(ctx, a.JobID)
		if err != nil {
			return err
		}
		if _, err := c.st.TransitionJob(ctx, a.JobID, types.Pending, types.Scheduled, w.now); err != nil {
			return fmt.Errorf("tick %d place %s: %w", w.tick, a.JobID, err)
		}
		if _, err := c.st.TransitionJob(ctx, a.JobID, types.Scheduled, types.Running, w.now); err != nil {
			return fmt.Errorf("tick %d place %s: %w", w.tick, a.JobID, err)
		}
		token := fmt.Sprintf("%s/%d", j.ID, j.Attempt)
		c.allocs[a.JobID] = a.Clone()
		c.placedAt[token] = w.tick
		for i, nid := range a.NodeIDs {
			v := c.views[nid]
			v.allocated = v.allocated.Add(j.Request)
			// Dispatch only lands if the agent is actually reachable
			// and running; against a dead, paused, or partitioned
			// agent the RPC fails silently from the controller's view
			// (it still books RUNNING), and either the failure
			// detector or heartbeat reconciliation requeues the job.
			agent := w.agents[nid]
			if agent.alive && !agent.paused && !agent.partitioned {
				agent.execs[token] = &execution{
					jobID: j.ID, attempt: j.Attempt, req: j.Request.Clone(),
					needed: w.jobs[j.ID].trueRun, primary: i == 0,
				}
			}
		}
		w.checker.jobStarted(a.JobID, w.tick)
	}
	return nil
}

func sortedJobIDs(m map[string]*chaosJob) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sortedAllocJobs(m map[string]types.Allocation) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func parseToken(token string) (string, int, error) {
	for i := len(token) - 1; i >= 0; i-- {
		if token[i] == '/' {
			var n int
			_, err := fmt.Sscanf(token[i+1:], "%d", &n)
			return token[:i], n, err
		}
	}
	return "", 0, fmt.Errorf("malformed token %q", token)
}
