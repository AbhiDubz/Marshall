package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// AgentClient is the dispatch-relevant slice of the agent RPC surface.
// Start MUST be idempotent by token: re-sending a token the agent has
// already accepted returns success without a second execution.
type AgentClient interface {
	Start(ctx context.Context, req StartRequest) error
	Probe(ctx context.Context, token string) (known bool, err error)
	Kill(ctx context.Context, token string) error
}

// StartRequest is what the leader sends the (primary) agent.
type StartRequest struct {
	Token   string
	JobID   string
	Attempt int
	NodeIDs []string
	Cmd     string
}

// JobStore is the store slice the dispatcher needs.
type JobStore interface {
	GetJob(ctx context.Context, id string) (types.Job, error)
	TransitionJob(ctx context.Context, id string, from, to types.JobState, at time.Time) (types.Job, error)
}

// Token composes the idempotency token for a job attempt.
func Token(jobID string, attempt int) string { return jobID + "/" + strconv.Itoa(attempt) }

// ParseToken splits a token into (jobID, attempt).
func ParseToken(token string) (string, int, error) {
	i := strings.LastIndexByte(token, '/')
	if i < 0 {
		return "", 0, fmt.Errorf("malformed token %q", token)
	}
	n, err := strconv.Atoi(token[i+1:])
	if err != nil {
		return "", 0, fmt.Errorf("malformed token %q: %v", token, err)
	}
	return token[:i], n, nil
}

// Dispatcher performs exactly-once dispatch for a leading control
// plane and reconciliation after failover.
type Dispatcher struct {
	WAL   WAL
	Store JobStore
	// Agent resolves the primary agent for a placement (NodeIDs[0]).
	Agent func(nodeID string) AgentClient
	// Cmd resolves a job's payload.
	Cmd func(ctx context.Context, jobID string) (string, error)
	// Now is the clock (injected for determinism).
	Now func() time.Time
}

// dispatchExactlyOnce dispatches one SCHEDULED job to its agents such
// that across any control-plane crash and failover the job neither
// vanishes nor runs twice.
//
// The sequence (every step durable or idempotent, in this exact
// order):
//
//  1. PRECONDITION. The job must be SCHEDULED with job.Attempt
//     matching the attempt this call is dispatching; the token is
//     Token(job.ID, job.Attempt). If the store shows anything else,
//     return an error without side effects (a newer leader or a
//     cancel got there first).
//
//  2. WAL INTENT. Append DISPATCH_INTENT{token, job, nodeIDs} and
//     require it durable BEFORE any message leaves the process. A
//     leader that dies before this step has done nothing; the job is
//     still SCHEDULED and any future leader may re-dispatch freely.
//     A duplicate INTENT for the same token (this call retried after
//     a lost ack) is harmless — recovery deduplicates by token.
//
//  3. AGENT START. Resolve the primary agent (NodeIDs[0]) and call
//     Start. Start is idempotent by token — the agent's dedupe table
//     guarantees at most one execution per token no matter how many
//     times this step runs. Three outcomes:
//     - success: the agent accepted (is running or already ran it);
//     - error, leader alive: return the error; the caller requeues
//     (SCHEDULED -> PENDING bumps Attempt, minting a fresh token,
//     and heartbeat fencing kills any zombie of the old token);
//     - leader dies mid-call: the ambiguous window. The INTENT in
//     the WAL is exactly the breadcrumb the next leader needs:
//     Recover probes the agent to learn whether Start landed.
//
//  4. WAL COMMIT. Append DISPATCH_COMMIT{token}. From here on every
//     future leader knows the agent has the job, even if the store
//     update below never happens.
//
//  5. STORE. TransitionJob(SCHEDULED -> RUNNING). CAS semantics: if a
//     concurrent actor moved the job first, surface ErrConflict —
//     recovery treats the WAL, not this call, as the source of truth.
//
// Return nil only after all five steps. Any error leaves the WAL and
// store in a state Recover can reconcile without human help.
//
// Invariants the tests check (leader killed at EVERY step boundary,
// both before and after the step's effect applies — including after
// the agent accepted but before COMMIT was recorded):
//   - The agent executes the token at most once, ever.
//   - After Recover on a fresh dispatcher: the job is RUNNING, the
//     WAL holds an INTENT and a COMMIT for the token, and running
//     Recover again changes nothing (idempotent).
//   - No interleaving loses the job (stuck SCHEDULED with no
//     breadcrumb) or aborts a dispatch the agent accepted.
func (d *Dispatcher) dispatchExactlyOnce(ctx context.Context, job types.Job, nodeIDs []string) error {
	panic("not implemented")
}

// Recover reconciles WAL state after this process takes leadership.
// For every token, in LSN order, deduplicated:
//
//   - INTENT + COMMIT: ensure the store says RUNNING (the previous
//     leader may have died between COMMIT and the store update).
//   - INTENT only: if the job is no longer SCHEDULED at that attempt,
//     append ABORT (the dispatch is moot — cancelled or superseded).
//     Otherwise probe the agent: token known -> append COMMIT and
//     mark RUNNING; token unknown -> the Start never landed, so
//     re-run dispatchExactlyOnce with the same token.
//   - ABORT: nothing.
//
// A SCHEDULED job with no WAL intent is deliberately not Recover's
// concern: the old leader died before step 2, so nothing ever left the
// process — the job looks exactly like one whose dispatch never began,
// and the new leader's normal dispatch pipeline picks it up.
//
// Recover is idempotent and safe to run on every leadership change.
func (d *Dispatcher) Recover(ctx context.Context) error {
	recs, err := d.WAL.Scan(ctx)
	if err != nil {
		return err
	}
	intents := map[string]Record{}
	committed := map[string]bool{}
	aborted := map[string]bool{}
	for _, r := range recs {
		switch r.Kind {
		case KindIntent:
			if _, ok := intents[r.Token]; !ok {
				intents[r.Token] = r
			}
		case KindCommit:
			committed[r.Token] = true
		case KindAbort:
			aborted[r.Token] = true
		}
	}
	tokens := make([]string, 0, len(intents))
	for t := range intents {
		tokens = append(tokens, t)
	}
	sort.Strings(tokens)

	now := d.Now()
	for _, token := range tokens {
		if aborted[token] {
			continue
		}
		intent := intents[token]
		job, err := d.Store.GetJob(ctx, intent.JobID)
		if err != nil {
			return fmt.Errorf("recover %s: %w", token, err)
		}
		if committed[token] {
			if job.State == types.Scheduled && job.Attempt == intent.Attempt {
				if _, err := d.Store.TransitionJob(ctx, job.ID, types.Scheduled, types.Running, now); err != nil {
					return fmt.Errorf("recover %s: %w", token, err)
				}
			}
			continue
		}
		// Intent without commit: was the dispatch superseded?
		if job.State != types.Scheduled || job.Attempt != intent.Attempt {
			if _, err := d.WAL.Append(ctx, Record{
				Kind: KindAbort, Token: token, JobID: intent.JobID,
				Attempt: intent.Attempt, At: now,
			}); err != nil {
				return fmt.Errorf("recover %s: %w", token, err)
			}
			continue
		}
		// Ambiguous: did the previous leader's Start land?
		if len(intent.NodeIDs) == 0 {
			return fmt.Errorf("recover %s: intent has no nodes", token)
		}
		known, err := d.Agent(intent.NodeIDs[0]).Probe(ctx, token)
		if err != nil {
			return fmt.Errorf("recover %s: probe: %w", token, err)
		}
		if known {
			if _, err := d.WAL.Append(ctx, Record{
				Kind: KindCommit, Token: token, JobID: intent.JobID,
				Attempt: intent.Attempt, NodeIDs: intent.NodeIDs, At: now,
			}); err != nil {
				return fmt.Errorf("recover %s: %w", token, err)
			}
			if _, err := d.Store.TransitionJob(ctx, job.ID, types.Scheduled, types.Running, now); err != nil && !errors.Is(err, errAlreadyRunning) {
				return fmt.Errorf("recover %s: %w", token, err)
			}
			continue
		}
		// Start never reached the agent: safe to re-dispatch, same token.
		if err := d.dispatchExactlyOnce(ctx, job, intent.NodeIDs); err != nil {
			return fmt.Errorf("recover %s: redispatch: %w", token, err)
		}
	}
	return nil
}

// errAlreadyRunning is a sentinel some stores may use; the MemStore
// returns ErrConflict instead, which Recover treats as fatal — by the
// time Recover moves a SCHEDULED job it has already re-read the state,
// so a conflict is a real interleaving bug, not noise.
var errAlreadyRunning = errors.New("already running")
