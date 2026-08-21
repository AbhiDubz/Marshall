# Architecture decision records

One entry per significant choice. ADR-0007 through ADR-0011 cover the
six functions that were built test-first as documented stubs and then
implemented against their suites.

## ADR-0001: Determinism as a hard interface requirement

**Decision.** Every scheduling-path function takes `now time.Time` as a
parameter, uses injected `*rand.Rand` for any randomness, and sorts
before iterating any map or candidate set.

**Context.** Distributed-scheduler bugs are overwhelmingly ordering
bugs. If a decision can differ between two runs with identical inputs,
no failing chaos seed is replayable and no simulator number is
trustworthy.

**Consequences.** Slightly more ceremony (`JobLookup` injection,
canonical sort helpers in `types`). In exchange: byte-identical sim
reports (checked in CI by hashing two runs), and `marshal-chaos
--seed N` reproduces any campaign failure exactly.

## ADR-0002: Trace files carry true runtimes; the engine draws nothing

**Decision.** The generator (seeded) draws each job's *true* runtime at
generation time and commits it into the trace JSON. The engine replays
exactly what the file says; schedulers only ever see `EstRuntime`.

**Context.** The alternative — the engine drawing true runtimes at
replay time from a seed — makes a trace's results depend on (trace,
engine-seed) pairs and invites accidental divergence between runs used
in different comparisons.

**Consequences.** A committed trace is a complete experiment
definition. Estimate error (the thing backfill must survive) is a
property of the workload, fixed forever, identical for every scheduler
compared against it.

## ADR-0003: One store contract, two implementations, one test suite

**Decision.** `store.Store` has a Postgres implementation and an
in-memory implementation; a single conformance suite runs against both
(PG side skips loudly if no database is reachable).

**Context.** The sim and chaos harness need microsecond-cheap state;
the control plane needs durability. Two stores drift unless the same
tests pin their semantics — especially transition CAS behavior and
requeue Attempt bumps.

**Consequences.** MemStore is not a mock: it enforces the same state
machine and event-log semantics, so dispatch/chaos tests exercise real
store behavior.

## ADR-0004: Job state transitions are compare-and-swap, in the store

**Decision.** `TransitionJob(id, from, to)` updates only if the row is
still in `from`, validates the edge against the `types` state machine,
and appends the event in the same transaction. Callers never write
`state` directly.

**Context.** With failover, two control planes can briefly both think
they own a job. CAS makes the slower writer fail with `ErrConflict`
instead of silently rewinding state (`COMPLETED -> RUNNING`).

**Consequences.** Every caller must know the state it believes the job
is in — which is exactly the discipline exactly-once dispatch needs;
`Attempt` bumps ride the same transaction so fencing tokens can never
diverge from state history.

## ADR-0005: Leader election via Postgres advisory locks (v1)

**Decision.** One session-level advisory lock (`store.TryLead` /
`WaitLead`) elects the marshald leader. No etcd, no Raft.

**Context.** Postgres is already the durability root: if it is down,
marshal cannot make progress anyway, so a separate consensus system
would add an availability dependency without removing one.

**Tradeoffs accepted.**
- *Lease = session.* Leadership drops the instant the leader's DB
  connection dies — fast failover, but a network blip between leader
  and Postgres causes an election even though the leader was healthy.
- *No self-fencing.* A deposed leader partitioned from Postgres cannot
  know it was deposed. Mitigation: leadership grants no write
  authority by itself — every dispatch effect goes through the WAL and
  CAS job transitions on Postgres, so a deposed leader's writes fail
  with conflicts instead of corrupting state. The advisory lock only
  prevents duplicate *work*, not duplicate *authority*.
- *Single point of contention.* Fine at v1 scale (one leader, a few
  standbys); revisit if the control plane ever needs to shard.

**Alternative rejected.** etcd/raft leases: real fencing tokens and
Postgres-independent elections, at the cost of a second quorum system
to operate. Wrong trade for v1.

## ADR-0006: Dispatch WAL is separate from the event log

**Decision.** Exactly-once dispatch writes to its own append-only
`dispatch_wal` table (INTENT / COMMIT / ABORT by token) rather than
reusing the `events` audit log.

**Context.** The event log is an audit trail: humans and metrics read
it, and it records everything. Recovery must scan a small,
precisely-typed log where every record has protocol meaning; mixing
concerns would couple recovery correctness to audit noise.

**Consequences.** Two append-only tables with the same no-rewrite
trigger; recovery deduplicates by token, so retried appends whose acks
were lost are harmless. The WAL is the source of truth for "did the
agent accept?"; the store remains the source of truth for job state.

## ADR-0007: Bin packing scores normalized leftover, exact fits apart

**Decision.** `BinPackAllocator` treats exact fits as their own class
(always preferred), and otherwise scores candidates by
leftover-after-placement summed across dimensions, each normalized by
the node's capacity in that dimension. Lower leftover wins; ties break
by node ID.

**Context.** Classic bin packing wants "fullest node first", but
multi-dimensional resources make "fullest" ambiguous: 500m CPU free
means something different on a 4-core node than a 64-core node, and a
free GPU is worth more than either. Normalizing per dimension makes
the score scale-free, and including *all* capacity dimensions in the
leftover means a CPU-only job placed on a GPU node is charged for the
GPUs it would strand — GRES conservation with no special case.

**Alternatives rejected.** Dot-product alignment (Tetris-style) is
reported in the literature to pack better on adversarial mixes, but
needs a tuned weight vector; on the committed traces (measured, table
in README) the parameter-free normalized-leftover rule already beats
firstfit and bestfit on wait times at equal utilization, so the
simpler rule wins v1.

**Measured.** On uniform, binpack+backfill gives the best mean wait
of the whole matrix (1m40.926s vs 3m10.07s for fifo+firstfit).

## ADR-0008: EASY backfill with a single clamped reservation

**Decision.** One reservation, for the head of the queue only
(`computeReservation`); backfill candidates start iff they are
projected to finish by the reservation time or fit entirely on
unreserved nodes. Overrunning jobs project their release at `now`,
never in the past; jobs the lookup cannot resolve never release.
Reservations are recomputed from scratch every cycle.

**Context.** Estimates are user-supplied lies. Persisting reservations
across cycles lets one bad estimate corrupt every later decision;
recomputing each cycle bounds the damage to a single window. The
`max(release, now)` clamp is what keeps an overrun from creating a
reservation in the past — which would otherwise let arbitrarily long
jobs "backfill" (they trivially finish after a past deadline is
un-delayable... in practice it grants phantom resources; the suite
pins this).

**Alternatives rejected.** Conservative backfill (a reservation for
every queued job) protects more jobs but costs a projection per
queued job per cycle and was not implemented here; EASY is SLURM's
default for the same cost/benefit reason, and the wins in the table
below came from EASY alone.

**Measured.** bimodal mean wait: 21m30s (fifo) → 47.6s (backfill),
p95 1h1m58s → 11m44s, utilization 0.5711 → 0.6371 (firstfit).

## ADR-0009: Gang scheduling by sticky hold accumulation

**Decision.** The highest-priority pending gang job claims free,
fitting nodes into a persistent hold (ID order), keeps them across
cycles, and starts only when the hold reaches NodeCount. Held nodes
take no other work; single-node jobs fill everything else greedily.
One accumulating gang job at a time.

**Context.** Without reservation, a k-node gang job needs k
simultaneously free nodes — probability ~zero under sustained load:
the starvation regression test shows a naive scheduler *never* starts
the 4-node gang job, while accumulation starts it 9s in. Stickiness
is the ratchet that converts "eventually k nodes free up" into a
bounded wait (~sum of the longest small jobs).

**Tradeoff accepted.** Held nodes idle while the hold fills — that is
the price of the bound, paid only when gang jobs queue. The bimodal
numbers show the price is negative in aggregate: gang+bestfit reaches
0.7052 utilization vs 0.6040 for fifo+bestfit, because large jobs
stop pinballing off transient fragmentation.

**Alternatives rejected.** Time-based coscheduling windows (start a
timer, release holds on expiry) adds a tuning knob without improving
the bound; strict FIFO head-blocking gets the same bound by freezing
the whole cluster, which the utilization numbers punish.

## ADR-0010: Preemption picks minimal victim sets, greedily per node

**Decision.** Only strictly-lower-priority jobs are preemptible. For
each slot of the pending job, every node's cost is the shortest
prefix of its victim list (sorted priority asc, StartAt desc, ID)
that frees enough room; the cheapest (victim count, priority sum,
node ID) node wins, its victims release on *every* node they occupy,
and the final placement must be verified by the allocator or nobody
is preempted.

**Context.** True minimum-victim selection is set-cover (NP-hard);
the greedy per-node prefix is exact for the common cases the suite
pins (one big victim beats two small ones; one gang victim frees two
slots) and never returns a partial answer. Strict inequality on
priority makes cascades structurally impossible: preemption chains
strictly descend, and requeued victims cannot touch their preemptor
or each other — the suite asserts this end to end.

**Alternatives rejected.** Cost = remaining runtime (checkpoint-aware
preemption) needs runtime data the system defines as unreliable;
StartAt-desc ("kill the youngest") is the assumption-free proxy for
least work lost.

## ADR-0011: Exactly-once dispatch = WAL sequencing + idempotent agents

**Decision.** Dispatch is INTENT (durable) → agent Start (idempotent
by token `jobID/attempt`) → COMMIT (durable) → CAS state change.
Recovery replays the WAL: committed intents are finished, uncommitted
ones are resolved by *probing the agent* — known token means commit,
unknown means re-dispatch with the same token. A SCHEDULED job with
no intent belongs to the normal dispatch pipeline, not recovery.

**Context.** Exactly-once is impossible with one message, possible
with an idempotency key and a durable outbox. The token doubles as
the fencing attempt number, so the WAL, the store's CAS transitions,
and heartbeat fencing all agree on which attempt is authoritative.
The crash matrix kills the leader before and after every side effect;
the ambiguous window (agent accepted, commit unrecorded) is exactly
why Probe exists — the agent's dedupe table is the tiebreaker of
record.

**Alternatives rejected.** Two-phase commit with the agent as a
participant adds a blocking protocol where an idempotent retry
suffices; store-side "dispatching" flags without a WAL cannot
distinguish "never sent" from "sent, ack lost", which is the whole
problem.

## ADR-0012: When backfill hurts

**Decision.** TODO — author to complete.

**Context.** On the bursty trace, EASY backfill with the firstfit
allocator scores *lower* CPU utilization and a *longer* makespan than
plain FIFO with the same allocator: 0.5104 vs 0.5218 and 46m1.278s vs
45m1.005s. Only its mean wait improves (32.24s vs 38.607s); its p95
wait is slightly worse (3m59.623s vs 3m54.352s). With bestfit or
binpack, backfill matches FIFO exactly on utilization and makespan on
this trace (0.5218 / 45m1.005s) and improves only the wait columns.

Relevant rows, from the committed `marshal-sim --compare` run in the
README (seed-42 traces, 200 jobs each):

| Trace | Scheduler | Allocator | Util (CPU) | Mean wait | P95 wait | Makespan |
|---|---|---|---|---|---|---|
| bursty | fifo | firstfit | 0.5218 | 38.607s | 3m54.352s | 45m1.005s |
| bursty | backfill | firstfit | **0.5104** | 32.24s | 3m59.623s | **46m1.278s** |
| bursty | fifo | bestfit | 0.5218 | 43.2s | 4m27.536s | 45m1.005s |
| bursty | backfill | bestfit | 0.5218 | 32.717s | 3m19.499s | 45m1.005s |
| bursty | fifo | binpack | 0.5218 | 41.112s | 3m55.077s | 45m1.005s |
| bursty | backfill | binpack | 0.5218 | 32.729s | 3m47.696s | 45m1.005s |

The same trace shows a second firstfit-specific degradation in the
gang row (gang+firstfit: 0.5038 / 46m37.855s, vs 0.5218 / 45m1.005s
for gang+bestfit and gang+binpack); it is recorded here as a
neighbouring observation only. No other trace shows backfill behind
FIFO on any column.

**Consequences.** TODO — author to complete.

## ADR-0013: The unreachable verify in SelectVictims

**Decision.** TODO — author to complete.

**Context.** Step 4 of the `SelectVictims` contract
(`pkg/preempt/policy.go`) requires that, after the per-slot greedy
selection has committed its victims, the allocator is re-run on the
projected node state (victims' allocations released) and must place
`pending`; otherwise nobody is preempted. The implementation performs
this check as its final step:

```go
// Verify: with victims released, the allocator must place pending.
if _, ok := p.Alloc.Fit(work, pending.Request, nc); !ok {
	return nil, false
}
```

Measured: deleting those four lines leaves the entire test suite
green. Verified on 2026-08-21 at commit `de9dccb` (all six functions
implemented, `TestSelectVictimsNoPartialPreemption` included):
`go test ./... -count=1` passes every package with the check removed.

Why no test can reach a failing verify, as the code stands today:

- Step 3 already returns `(nil, false)` whenever any slot finds no
  feasible node. When the slot loop completes, `chosen` holds
  `nc` distinct, non-draining nodes, each of which satisfied
  `pending.Request.Fits(avail)` after the releases committed so far —
  and later releases only ever increase a node's availability.
- `Fit` has identical feasibility semantics across firstfit, bestfit,
  and binpack. All three build the same candidate set
  (`alloc.candidates`: every node with `CanFit(req)`, i.e. not
  draining and `req.Fits(Available())`) and return `false` exactly
  when fewer than `nodeCount` candidates exist. The per-slot loops in
  bestfit and binpack charge only the node they pick and never reuse
  it, so once `len(cands) >= nodeCount` they cannot fail. The three
  differ in *which* nodes they choose, never in *whether* a placement
  exists.
- The suite's no-partial-preemption case
  (`TestSelectVictimsNoPartialPreemption`, added alongside this
  record) is therefore satisfied by the step-3 early return, not by
  the step-4 verify.

The check is currently unreachable defensive code: it guards against
an allocator whose feasibility differs from `CanFit`-per-node, which
none of the three registered allocators is.

**Consequences.** TODO — author to complete.
