package dispatch

// Test suite for stub #6: Dispatcher.dispatchExactlyOnce. The crash
// matrix kills the "leader" at every step boundary of the dispatch
// sequence — before and after each side effect applies, including the
// window after the agent accepted but before the leader durably
// recorded the commit — then lets a fresh dispatcher Recover and
// asserts the job neither vanishes nor runs twice.
//
// These tests FAIL until the stub is implemented; helpers convert the
// stub's panic into a test failure. Recovery-only tests (no stub in
// the path) pass already.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/store"
	"github.com/AbhiDubz/Marshall/pkg/types"
)

var (
	errCrash = errors.New("simulated leader death")
	tnow     = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

// crasher simulates leader death at a chosen side-effecting operation.
// after=false: the leader dies before the op applies (message never
// sent / row never written). after=true: the op applies but the leader
// dies before seeing the result (the ambiguous-ack window).
type crasher struct {
	steps   int
	crashAt int
	after   bool
	dead    bool
}

func (c *crasher) op(apply func()) error {
	if c == nil {
		apply()
		return nil
	}
	if c.dead {
		return errCrash
	}
	c.steps++
	if c.steps == c.crashAt && !c.after {
		c.dead = true
		return errCrash
	}
	apply()
	if c.steps == c.crashAt && c.after {
		c.dead = true
		return errCrash
	}
	return nil
}

type crashWAL struct {
	inner WAL
	c     *crasher
}

func (w crashWAL) Append(ctx context.Context, rec Record) (int64, error) {
	var (
		lsn int64
		err error
	)
	if cerr := w.c.op(func() { lsn, err = w.inner.Append(ctx, rec) }); cerr != nil {
		return 0, cerr
	}
	return lsn, err
}

func (w crashWAL) Scan(ctx context.Context) ([]Record, error) {
	if w.c != nil && w.c.dead {
		return nil, errCrash
	}
	return w.inner.Scan(ctx)
}

type crashStore struct {
	inner JobStore
	c     *crasher
}

func (s crashStore) GetJob(ctx context.Context, id string) (types.Job, error) {
	if s.c != nil && s.c.dead {
		return types.Job{}, errCrash
	}
	return s.inner.GetJob(ctx, id)
}

func (s crashStore) TransitionJob(ctx context.Context, id string, from, to types.JobState, at time.Time) (types.Job, error) {
	var (
		j   types.Job
		err error
	)
	if cerr := s.c.op(func() { j, err = s.inner.TransitionJob(ctx, id, from, to, at) }); cerr != nil {
		return types.Job{}, cerr
	}
	return j, err
}

// agentState is the durable node-side state (dedupe table, execution
// counts); it survives leader death. fakeAgent is one leader's client
// view of it, carrying that leader's crasher.
type agentState struct {
	mu    sync.Mutex
	known map[string]bool
	execs map[string]int
}

func newAgentState() *agentState {
	return &agentState{known: make(map[string]bool), execs: make(map[string]int)}
}

func (s *agentState) execCount(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execs[token]
}

func (s *agentState) markAccepted(token string, executed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.known[token] = true
	if executed {
		s.execs[token]++
	}
}

type fakeAgent struct {
	s *agentState
	c *crasher
}

func (a fakeAgent) Start(_ context.Context, req StartRequest) error {
	return a.c.op(func() {
		a.s.mu.Lock()
		defer a.s.mu.Unlock()
		if a.s.known[req.Token] {
			return // idempotent: accepted before, no second execution
		}
		a.s.known[req.Token] = true
		a.s.execs[req.Token]++
	})
}

func (a fakeAgent) Probe(_ context.Context, token string) (bool, error) {
	if a.c != nil && a.c.dead {
		return false, errCrash
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	return a.s.known[token], nil
}

func (a fakeAgent) Kill(context.Context, string) error { return nil }

// world is one test scenario's shared durable state — it survives
// "leader death" the way Postgres and the agent survive a marshald
// crash.
type world struct {
	wal   *MemWAL
	st    *store.MemStore
	agent *agentState
	job   types.Job
}

func newWorld(t *testing.T) *world {
	t.Helper()
	ctx := context.Background()
	st := store.NewMemStore()
	job := types.Job{
		ID: "j1", User: "alice", Priority: 5,
		Request:    types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 1 << 30},
		NodeCount:  1,
		EstRuntime: time.Minute,
		SubmitAt:   tnow,
		State:      types.Pending,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := st.TransitionJob(ctx, "j1", types.Pending, types.Scheduled, tnow); err != nil {
		t.Fatal(err)
	}
	j, _ := st.GetJob(ctx, "j1")
	return &world{wal: NewMemWAL(), st: st, agent: newAgentState(), job: j}
}

func (w *world) dispatcher(c *crasher) *Dispatcher {
	return &Dispatcher{
		WAL:   crashWAL{inner: w.wal, c: c},
		Store: crashStore{inner: w.st, c: c},
		Agent: func(string) AgentClient { return fakeAgent{s: w.agent, c: c} },
		Cmd:   func(context.Context, string) (string, error) { return "true", nil },
		Now:   func() time.Time { return tnow },
	}
}

// deo invokes the stub, converting its panic into a test failure.
// A returned errCrash is the simulated death — expected, not a bug.
func deo(t *testing.T, d *Dispatcher, job types.Job, nodeIDs []string) error {
	t.Helper()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("dispatchExactlyOnce panicked: %v (stub #6 — implement it)", r)
			}
		}()
		err = d.dispatchExactlyOnce(context.Background(), job, nodeIDs)
	}()
	return err
}

func recoverOrFail(t *testing.T, d *Dispatcher) {
	t.Helper()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Recover panicked: %v (stub #6 dispatchExactlyOnce — implement it)", r)
			}
		}()
		if err := d.Recover(context.Background()); err != nil {
			t.Fatalf("Recover: %v", err)
		}
	}()
}

func (w *world) walHasIntent(token string) bool {
	recs, _ := w.wal.Scan(context.Background())
	for _, r := range recs {
		if r.Token == token && r.Kind == KindIntent {
			return true
		}
	}
	return false
}

func (w *world) assertExactlyOnce(t *testing.T, scenario string) {
	t.Helper()
	ctx := context.Background()
	token := Token("j1", 0)

	if n := w.agent.execCount(token); n != 1 {
		t.Fatalf("%s: token executed %d times, want exactly 1", scenario, n)
	}
	job, err := w.st.GetJob(ctx, "j1")
	if err != nil {
		t.Fatalf("%s: job vanished: %v", scenario, err)
	}
	if job.State != types.Running || job.Attempt != 0 {
		t.Fatalf("%s: job is %s attempt %d, want RUNNING attempt 0", scenario, job.State, job.Attempt)
	}
	recs, err := w.wal.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var intents, commits, aborts int
	for _, r := range recs {
		if r.Token != token {
			continue
		}
		switch r.Kind {
		case KindIntent:
			intents++
		case KindCommit:
			commits++
		case KindAbort:
			aborts++
		}
	}
	if intents < 1 || commits < 1 || aborts != 0 {
		t.Fatalf("%s: WAL wrong: intents=%d commits=%d aborts=%d", scenario, intents, commits, aborts)
	}
}

func TestDispatchHappyPath(t *testing.T) {
	w := newWorld(t)
	d := w.dispatcher(nil)
	if err := deo(t, d, w.job, []string{"n1"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	w.assertExactlyOnce(t, "happy path")
}

// TestExactlyOnceAcrossLeaderDeathEverywhere is the required crash
// matrix. Side-effecting ops in sequence order: 1=WAL INTENT append,
// 2=agent Start, 3=WAL COMMIT append, 4=store SCHEDULED->RUNNING.
// crashAt=2/after=true is the brief's called-out window: the agent
// accepted but the leader died before durably recording it.
func TestExactlyOnceAcrossLeaderDeathEverywhere(t *testing.T) {
	for crashAt := 1; crashAt <= 5; crashAt++ {
		for _, after := range []bool{false, true} {
			scenario := fmt.Sprintf("crashAt=%d after=%v", crashAt, after)
			t.Run(scenario, func(t *testing.T) {
				w := newWorld(t)
				c := &crasher{crashAt: crashAt, after: after}
				err := deo(t, w.dispatcher(c), w.job, []string{"n1"})
				if crashAt <= 4 && err == nil {
					t.Fatalf("%s: expected the simulated crash to surface an error", scenario)
				}

				// Failover: fresh leader over the same durable state.
				recoverOrFail(t, w.dispatcher(nil))

				// A crash before the INTENT landed leaves a SCHEDULED
				// job with no WAL breadcrumb — by design that is the
				// dispatch pipeline's case, not Recover's: the new
				// leader's next cycle re-dispatches it.
				if job, _ := w.st.GetJob(context.Background(), "j1"); job.State == types.Scheduled &&
					!w.walHasIntent(Token("j1", 0)) {
					if err := deo(t, w.dispatcher(nil), job, []string{"n1"}); err != nil {
						t.Fatalf("%s: pipeline re-dispatch: %v", scenario, err)
					}
				}
				w.assertExactlyOnce(t, scenario)

				// Recovery must be idempotent.
				recoverOrFail(t, w.dispatcher(nil))
				w.assertExactlyOnce(t, scenario+" (second recover)")
			})
		}
	}
}

func TestDispatchDuplicateCallDoesNotDoubleRun(t *testing.T) {
	w := newWorld(t)
	d := w.dispatcher(nil)
	if err := deo(t, d, w.job, []string{"n1"}); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	// A confused caller re-dispatches the same attempt. Whatever the
	// return value, the token must not execute again.
	_ = deo(t, d, w.job, []string{"n1"})
	w.assertExactlyOnce(t, "duplicate call")
}

// --- recovery-only behavior (implemented; passes before the stub) ---

func TestRecoverAbortsSupersededIntent(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	token := Token("j1", 0)
	if _, err := w.wal.Append(ctx, Record{
		Kind: KindIntent, Token: token, JobID: "j1", Attempt: 0, NodeIDs: []string{"n1"}, At: tnow,
	}); err != nil {
		t.Fatal(err)
	}
	// The job was requeued before the old leader got further: it is
	// now PENDING at attempt 1 — the intent is moot.
	if _, err := w.st.TransitionJob(ctx, "j1", types.Scheduled, types.Pending, tnow); err != nil {
		t.Fatal(err)
	}
	recoverOrFail(t, w.dispatcher(nil))

	if n := w.agent.execCount(token); n != 0 {
		t.Fatalf("aborted dispatch must never start: %d executions", n)
	}
	recs, _ := w.wal.Scan(ctx)
	foundAbort := false
	for _, r := range recs {
		if r.Token == token && r.Kind == KindAbort {
			foundAbort = true
		}
	}
	if !foundAbort {
		t.Fatal("superseded intent must be ABORTed in the WAL")
	}
}

func TestRecoverCommitsAcceptedButUnrecordedDispatch(t *testing.T) {
	// The exact window the brief calls out, reconstructed by hand:
	// INTENT in the WAL, the agent accepted, no COMMIT anywhere, and
	// the store still says SCHEDULED. Recovery alone must finish the
	// job's paperwork without a second execution.
	w := newWorld(t)
	ctx := context.Background()
	token := Token("j1", 0)
	if _, err := w.wal.Append(ctx, Record{
		Kind: KindIntent, Token: token, JobID: "j1", Attempt: 0, NodeIDs: []string{"n1"}, At: tnow,
	}); err != nil {
		t.Fatal(err)
	}
	w.agent.markAccepted(token, true)

	recoverOrFail(t, w.dispatcher(nil))
	w.assertExactlyOnce(t, "accepted-but-unrecorded")
}

func TestTokenRoundTrip(t *testing.T) {
	id, attempt, err := ParseToken(Token("job-x", 7))
	if err != nil || id != "job-x" || attempt != 7 {
		t.Fatalf("got %q %d %v", id, attempt, err)
	}
	if _, _, err := ParseToken("nope"); err == nil {
		t.Fatal("malformed token must error")
	}
}
