# Architecture decision records

One entry per significant choice. ADRs for the six stubbed algorithms
are deliberately absent: writing those is the project owner's job after
implementing each stub (the reasoning is the artifact).

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
