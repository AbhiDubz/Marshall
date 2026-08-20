# Chaos harness

`pkg/chaos` runs the whole control loop — controller, agents, network —
as a deterministic discrete-time simulation (500ms ticks) under seeded
fault injection, with invariants checked every tick. A run is a pure
function of (config, seed): **any failing seed replays byte-for-byte**.

```bash
go build -o bin/marshal-chaos ./cmd/marshal-chaos
./bin/marshal-chaos --seeds 1000      # campaign
./bin/marshal-chaos --seed 137 -v     # exact replay of one seed, with its fault plan
```

CI runs a 200-seed campaign on every push.

## Fault model

Each seed draws a plan (`GeneratePlan`) of per-node faults, all healed
before a third of the horizon so runs can converge:

| Fault | Models | Effect in the world |
|---|---|---|
| `kill` | agent crash + restart | executions lost, no heartbeats, node returns empty |
| `partition` | network split | agent keeps running (zombies); traffic cut both ways, including in-flight; completions retried on heal |
| `pause` | SIGSTOP / GC stall | no heartbeats, no progress, resources still physically held |
| `hb-drop` | lossy network | each heartbeat in the window dropped with probability p |
| `hb-delay` | congested network | heartbeats delivered late (stale observations) |
| `clock-skew` | bad NTP | agent's heartbeat cadence runs fast or slow all run |

## Invariants (checked continuously)

- **no-double-run** — a job is never accepted as completed twice; no
  execution exists for an attempt newer than the store's; live shards
  of one attempt never exceed `NodeCount`.
- **no-double-booking** — ground truth: the executions physically on a
  node never exceed its capacity. Books: the controller's per-node
  accounting reconciles exactly with the store's RUNNING set.
- **no-job-lost** — at the end of the horizon every job is COMPLETED
  and was accepted exactly once.
- **no-starvation** — no PENDING job waits beyond `--start-bound`
  (default 60m) since its last (re)enqueue.
- **legal-transitions** — every state change goes through
  `store.TransitionJob`'s CAS + state machine; an illegal edge anywhere
  fails the run immediately.

The checker's teeth are themselves tested: a deliberately evil
scheduler that double-books trips `no-double-booking`, and a scheduler
that never schedules trips `no-starvation`
(`TestCheckerCatchesDoubleBooking`, `TestCheckerCatchesStarvation`).

## Correctness mechanisms the harness forced into the design

These came out of failing campaign seeds, not foresight (each was a
real replayed failure during development):

1. **Fencing by attempt.** Heartbeats list running tokens
   (`jobID/attempt`); the controller kills any token that no longer
   matches a RUNNING job at that attempt. Stale completions are
   ignored the same way — execution is at-least-once, *acceptance* is
   exactly-once.
2. **Readmission.** A node back from the dead takes no new work until
   its reported tokens are all fenced — zombies still hold real
   memory and cores.
3. **Completing state.** When a job is requeued while some of its old
   nodes are still alive (a gang shard's node died, the rest didn't),
   those nodes take no new work until they confirm the old shard is
   dead. Without this, the replacement attempt can land on top of its
   own zombie (found by seed 8's original failure).
4. **Stale-observation guards.** A delayed heartbeat sent before a
   placement cannot count as evidence the execution is missing;
   reconciliation only trusts heartbeats sent after `placedAt`.
   Without this, two delayed heartbeats requeue a healthy job onto its
   own node (also seed 8).
5. **Heartbeat reconciliation.** A node that restarts faster than the
   failure detector notices reports no tokens; after two consecutive
   heartbeats missing an expected token (grace for in-flight
   completion reports), the job is requeued.

## Model simplifications (deliberate)

- Dispatch is synchronous within a tick; the exactly-once *WAL*
  protocol is tested separately and exhaustively in `pkg/dispatch`
  (leader death at every step), not re-modeled here.
- Gang jobs complete when their primary shard completes; a gang
  barrier protocol is out of scope.
- Controller failover is not simulated (single controller); leader
  election is tested in `pkg/store`, dispatch failover in
  `pkg/dispatch`.

## Replaying a failure

A campaign failure prints `seed N: FAIL: seed N tick T: INVARIANT ...`.
Run `./bin/marshal-chaos --seed N -v` to reproduce the identical run
and print the fault plan. Everything — workload, faults, message
timing, scheduler decisions — re-derives from the seed.
