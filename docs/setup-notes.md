# Setup notes — what fought back

Environment: macOS on Apple Silicon (arm64), Darwin 25.5.0. This file
records every deviation from the brief's setup script and why.

## No Homebrew, no admin

The machine had no Homebrew and installing it needs an admin password,
so `brew install go postgresql@16 kubectl helm` was not possible as
written. Deviations:

- **Go**: official toolchain tarball from go.dev installed user-space.
  `go1.27.0.darwin-arm64.tar.gz` extracted to `~/sdk/go1.27.0`; add
  `~/sdk/go1.27.0/bin` to `PATH`. Satisfies the "Go 1.22+" requirement.
- **PostgreSQL 16**: `brew services start postgresql@16` replaced with
  a Docker container (Docker Desktop was already installed):

  ```
  docker run -d --name marshal-postgres \
    -e POSTGRES_USER=marshal -e POSTGRES_PASSWORD=marshal -e POSTGRES_DB=marshal \
    -p 5433:5432 postgres:16
  ```

  Host port **5433** (not 5432) to avoid colliding with anything else.
  The store test suite connects to
  `postgres://marshal:marshal@localhost:5433/marshal?sslmode=disable`
  by default; override with `MARSHAL_TEST_DSN`. If Postgres is
  unreachable the PG conformance tests log a loud SKIP and the
  in-memory conformance tests still run.

## Cluster tooling

- `container` (Apple's native container CLI) is not installed and needs
  brew → skipped. `colima`/`k3d` likewise unavailable without brew.
- Per the brief, M0/M1 run entirely against the in-process simulator,
  which needs no cluster. M0.5's daemons are built and unit-tested
  locally; the k8s manifests under `deploy/` are written for a 4-node
  cluster and were validated with `kubectl --dry-run=client` only,
  since no cluster runtime could be installed without admin rights.
  Wire them up on a machine with brew by following `deploy/README.md`.
- `kubectl` and `docker` were already present and are used.

## protoc

`protoc` is not brew-installable here either; the official release
binary from github.com/protocolbuffers/protobuf is downloaded to
`~/sdk/protoc` by `deploy/tools.sh` (checked in), and the Go plugins
are installed with `go install`. Generated gRPC code is committed under
`pkg/rpc/marshalpb/` so builds never require protoc.
