# marshal

A distributed batch job scheduler modeled on SLURM, in Go: a Postgres-
backed control plane, node agents, and a deterministic in-memory
cluster simulator where the scheduling algorithms are developed and
measured.

Six functions were built test-first: each began as a documented stub
with a complete failing suite specifying its algorithm and invariants,
then was implemented against that suite. All six are done and
`go test ./...` is fully green.

| # | Function | Spec + suite | Decision record |
|---|---|---|---|
| 1 | `alloc.BinPackAllocator.Fit` | `pkg/alloc/binpack_test.go` | ADR-0007 |
| 2 | `sched.BackfillScheduler.Schedule` | `pkg/sched/backfill_test.go` | ADR-0008 |
| 3 | `sched.BackfillScheduler.computeReservation` | `pkg/sched/backfill_test.go` | ADR-0008 |
| 4 | `sched.GangScheduler.accumulate` | `pkg/sched/gang_test.go` | ADR-0009 |
| 5 | `preempt.Policy.SelectVictims` | `pkg/preempt/policy_test.go` | ADR-0010 |
| 6 | `dispatch.Dispatcher.dispatchExactlyOnce` | `pkg/dispatch/dispatch_test.go` | ADR-0011 |

## Architecture

```
                 ┌────────────┐   submit/status/cancel   ┌────────────┐
     users ───▶  │ marshalctl │ ───────gRPC─────────────▶│  marshald   │
                 └────────────┘                          │ control     │
                                                         │ plane       │
                        ┌────────────────────────────────│ (leader)    │
                        │        dispatch (exactly-once) └──────┬──────┘
                        ▼                                       │
                 ┌──────────────┐    heartbeats / status        │
                 │ marshal-agent│ ◀─────────────────────────────┘
                 │  (per node)  │                        ┌──────▼──────┐
                 └──────────────┘                        │  Postgres   │
                     runs jobs                           │ jobs, WAL,  │
                     as containers                       │ event log   │
                                                         └─────────────┘

     marshal-sim: the same schedulers/allocators against a virtual
     clock and simulated nodes — no cluster, fully deterministic.
```

- `pkg/types` — Job, Node, ResourceSpec, Allocation, the job state machine
- `pkg/store` — Postgres store + append-only event log (+ in-memory twin)
- `pkg/alloc` — firstfit, bestfit, binpack allocators
- `pkg/sched` — fifo, EASY-backfill, and gang (reservation-accumulating) schedulers
- `pkg/preempt` — minimal-victim preemption policy + fair-share accounting
- `pkg/dispatch` — exactly-once dispatch protocol with WAL recovery
- `pkg/sim` — deterministic simulator, trace generator, replay
- `pkg/chaos` — seeded fault injection + invariant checker (see [docs/chaos.md](docs/chaos.md))
- `pkg/metrics` — Prometheus instrumentation (dashboard: `deploy/grafana/`)
- `cmd/marshal-chaos` — chaos campaigns: `--seeds 1000`, replay via `--seed N`

## Quickstart

```bash
# toolchain: Go 1.22+ (see docs/setup-notes.md for this machine's setup)
go test ./...          # fully green

# replay a committed trace deterministically
go build -o bin/marshal-sim ./cmd/marshal-sim
./bin/marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42

# same seed → byte-identical output
./bin/marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42 | sha256sum

# compare every scheduler × allocator over the committed traces
./bin/marshal-sim --compare --chart docs/compare.svg

# regenerate traces (committed ones use seed 42, 200 jobs)
./bin/marshal-sim --gen --kind bursty --seed 42 --jobs 200 --out traces/bursty.json

# chaos: 1000-seed fault-injection campaign (~25s); replay any failure exactly
go build -o bin/marshal-chaos ./cmd/marshal-chaos
./bin/marshal-chaos --seeds 1000
./bin/marshal-chaos --seed 137 -v
```

## Real cluster (local dev)

```bash
docker run -d --name marshal-postgres \
  -e POSTGRES_USER=marshal -e POSTGRES_PASSWORD=marshal -e POSTGRES_DB=marshal \
  -p 5433:5432 postgres:16

go build -o bin/marshald ./cmd/marshald
go build -o bin/marshal-agent ./cmd/marshal-agent
go build -o bin/marshalctl ./cmd/marshalctl

./bin/marshald &                                   # gRPC :7070, /metrics :9090
for i in 1 2 3 4; do
  ./bin/marshal-agent --node-id node-$i --listen :707$i --advertise localhost:707$i &
done

./bin/marshalctl submit --cmd "sleep 10"
./bin/marshalctl nodes
```

k8s manifests for the same 4-node shape are in `deploy/k8s/`; Grafana
dashboard JSON in `deploy/grafana/`; an Ansible playbook for real
worker nodes in `deploy/ansible/`; `Jenkinsfile` mirrors the GitHub
Actions gates (vet, stub-aware tests, determinism check, 200-seed
chaos campaign).

## Results

Every number below is a real run of `marshal-sim --compare` over the
committed seed-42 traces (200 jobs each); regenerate with
`./bin/marshal-sim --compare --chart docs/compare.svg`. Nothing here
is ever estimated.

| Trace | Scheduler | Allocator | Util (CPU) | Mean wait | P95 wait | Makespan |
|---|---|---|---|---|---|---|
| uniform | fifo | firstfit | 0.5318 | 3m10.07s | 10m2.358s | 1h4m57.205s |
| uniform | fifo | bestfit | 0.5337 | 3m27.246s | 11m6.387s | 1h4m43.4s |
| uniform | fifo | binpack | 0.5337 | 2m59.908s | 9m46.599s | 1h4m43.4s |
| uniform | backfill | firstfit | 0.5337 | 1m54.278s | 9m12.912s | 1h4m43.4s |
| uniform | backfill | bestfit | 0.5337 | 1m50.367s | 9m34.471s | 1h4m43.4s |
| uniform | backfill | binpack | 0.5337 | 1m40.926s | 8m35.806s | 1h4m43.4s |
| uniform | gang | firstfit | 0.5337 | 1m42.643s | 9m20.255s | 1h4m43.4s |
| uniform | gang | bestfit | 0.5337 | 1m44.07s | 9m5.36s | 1h4m43.4s |
| uniform | gang | binpack | 0.5337 | 1m39.153s | 8m38.262s | 1h4m43.4s |
| bimodal | fifo | firstfit | 0.5711 | 21m30.296s | 1h1m58.377s | 2h0m28.621s |
| bimodal | fifo | bestfit | 0.6040 | 24m16.374s | 1h4m21.103s | 1h53m54.69s |
| bimodal | fifo | binpack | 0.5987 | 20m52.968s | 59m59.561s | 1h54m55.997s |
| bimodal | backfill | firstfit | 0.6371 | 47.638s | 11m44.574s | 1h48m0.107s |
| bimodal | backfill | bestfit | 0.6244 | 56.401s | 13m9.292s | 1h50m11.617s |
| bimodal | backfill | binpack | 0.6010 | 43.564s | 24m19.294s | 1h54m29.502s |
| bimodal | gang | firstfit | 0.5823 | 1m15.516s | 17m5.338s | 1h58m9.746s |
| bimodal | gang | bestfit | **0.7052** | 37.678s | 8m21.891s | **1h25m11.576s** |
| bimodal | gang | binpack | 0.5823 | 1m15.469s | 17m5.338s | 1h58m9.746s |
| bursty | fifo | firstfit | 0.5218 | 38.607s | 3m54.352s | 45m1.005s |
| bursty | fifo | bestfit | 0.5218 | 43.2s | 4m27.536s | 45m1.005s |
| bursty | fifo | binpack | 0.5218 | 41.112s | 3m55.077s | 45m1.005s |
| bursty | backfill | firstfit | 0.5104 | 32.24s | 3m59.623s | 46m1.278s |
| bursty | backfill | bestfit | 0.5218 | 32.717s | 3m19.499s | 45m1.005s |
| bursty | backfill | binpack | 0.5218 | 32.729s | 3m47.696s | 45m1.005s |
| bursty | gang | firstfit | 0.5038 | 31.028s | 3m55.246s | 46m37.855s |
| bursty | gang | bestfit | 0.5218 | 31.413s | 3m19.499s | 45m1.005s |
| bursty | gang | binpack | 0.5218 | 31.473s | 3m47.696s | 45m1.005s |

Headlines: on the gang-heavy **bimodal** trace, EASY backfill cuts
mean wait from ~21–24 minutes (FIFO) to under a minute, and
reservation-accumulating gang scheduling with bestfit lifts CPU
utilization from 0.60 to **0.71** while shaving ~29 minutes off the
makespan. On **uniform**, backfill roughly halves mean wait. On
**bursty**, short bursts drain quickly whatever the policy — the
policies differ by seconds, not minutes.

Chaos: `marshal-chaos --seeds 1000` (fifo) and 200-seed campaigns
with `--sched backfill`, `--sched gang`, and `--alloc binpack` all
pass with zero invariant violations.

## Docs

- [docs/design.md](docs/design.md) — control plane, agent protocol, state machine
- [docs/decisions.md](docs/decisions.md) — ADRs
- [docs/chaos.md](docs/chaos.md) — fault model, invariants, replay
- [docs/setup-notes.md](docs/setup-notes.md) — environment deviations
