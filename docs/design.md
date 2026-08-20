# marshal design

## Components

- **marshald** — the control plane. Holds the scheduler loop, owns all
  writes to Postgres, dispatches work to agents. Exactly one instance
  is leader at a time (Postgres advisory-lock lease; see ADR-0005).
- **marshal-agent** — one per node. Registers on boot, heartbeats,
  executes jobs as containers, reports terminal status. Keeps an
  idempotency table of dispatch tokens so a retried dispatch never
  double-runs.
- **marshalctl** — CLI for submit / status / cancel / node list.
- **marshal-sim** — replays traces through the same scheduler and
  allocator code against a virtual clock. No cluster, no wall time, no
  unseeded randomness: identical inputs give byte-identical reports.

## Job state machine

```
 PENDING ──▶ SCHEDULED ──▶ RUNNING ──▶ COMPLETED (terminal)
    ▲            │            │
    │            │            ├──▶ FAILED ────▶ PENDING (Attempt+1)
    │            │            └──▶ PREEMPTED ─▶ PENDING (Attempt+1)
    │            └──▶ PENDING (dispatch failed, Attempt+1)
    └────────────────────────────────────────────┘
```

The transition table lives in `pkg/types` (`LegalTransition`); the
store enforces it on every write (`TransitionJob` is a compare-and-swap
on the `from` state) and appends an event recording the edge. There is
no path out of COMPLETED, and no way to re-enter RUNNING except through
SCHEDULED. Requeue edges (`* -> PENDING`) increment `Attempt`, which
doubles as the fencing token for exactly-once dispatch.

## Scheduling cycle

Every cycle the control plane (or the sim engine) hands the scheduler:

- `now` — explicit; scheduling logic never reads a clock,
- the PENDING queue sorted by submit time,
- a snapshot of nodes (capacity, allocation, draining, heartbeat),
- the currently running allocations.

The scheduler returns allocations to start immediately and must not
mutate its inputs. All schedulers share canonical ordering: priority
desc, submit asc, ID asc — so equal-priority ties always break by
submit time, deterministically.

- **fifo**: strict head-of-line blocking. The baseline.
- **backfill** (stubs #2/#3): EASY backfill — one reservation for the
  blocked head, later jobs may start only if they provably cannot
  delay it. Runtime estimates for running jobs come from a `JobLookup`
  injected at construction (the Schedule signature only carries the
  pending queue).
- **gang** (stub #4): all-or-nothing multi-node jobs accumulate node
  holds across cycles — the anti-starvation ratchet. Single-node jobs
  fill non-held capacity greedily.

Allocators (`Fit`) answer only "which nodes": firstfit (lowest IDs),
bestfit (tightest fit), binpack (stub #1 — fragmentation-minimizing).
All sort candidates before choosing; shuffled input never changes the
answer.

## Agent protocol (M0.5)

gRPC, four RPCs:

- `Register(node capacity) -> node ID` — agent boots, announces.
- `Heartbeat(node ID, allocated, running tokens) -> directives` —
  every 3s; the reply carries kill directives for fenced-out attempts.
  A node missing heartbeats for 15s is marked dead; its RUNNING jobs
  are requeued with Attempt+1.
- `Dispatch(token, job, node IDs, cmd)` (leader -> agent) — idempotent
  by token = `jobID/attempt`. The agent's dedupe table means "at most
  once per token" on the node; the WAL protocol (see below) turns that
  into exactly-once end to end.
- `Report(token, exit status)` — terminal status; the leader validates
  the token against the job's current Attempt and ignores stale ones.

## Exactly-once dispatch (M5, stub #6)

The WAL (Postgres table, append-only) records `DISPATCH_INTENT` before
any external effect and `DISPATCH_COMMIT` after the agent accepts.
Recovery scans for INTENT-without-COMMIT and probes the agent: token
known → commit and mark RUNNING; token unknown → safe re-dispatch with
the same token. The full sequencing contract is the doc comment on
`dispatch.Dispatcher.dispatchExactlyOnce`.

## Simulator

Event-driven: the virtual clock jumps between submit/finish events;
after draining all events at a timestamp the scheduler runs once. Jobs
run for their *true* runtime from the trace; schedulers only ever see
the user estimate. Utilization is the exact integral of allocated
resources over the makespan. Reports print with fixed field order and
precision, so `sha256sum` is a valid regression check.
