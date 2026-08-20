# Multi-stage build for all marshal binaries.
# Build a specific component with:
#   docker build -f deploy/Dockerfile --target marshald -t marshal/marshald .
#   docker build -f deploy/Dockerfile --target marshal-agent -t marshal/agent .
#   docker build -f deploy/Dockerfile --target marshalctl -t marshal/ctl .

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/marshald ./cmd/marshald \
 && CGO_ENABLED=0 go build -o /out/marshal-agent ./cmd/marshal-agent \
 && CGO_ENABLED=0 go build -o /out/marshalctl ./cmd/marshalctl \
 && CGO_ENABLED=0 go build -o /out/marshal-sim ./cmd/marshal-sim

FROM alpine:3 AS marshald
COPY --from=build /out/marshald /usr/local/bin/marshald
ENTRYPOINT ["marshald"]

FROM alpine:3 AS marshal-agent
COPY --from=build /out/marshal-agent /usr/local/bin/marshal-agent
# Jobs run via `sh -c` inside this container (exec runner) — the pod
# is the sandbox on k8s.
ENTRYPOINT ["marshal-agent"]

FROM alpine:3 AS marshalctl
COPY --from=build /out/marshalctl /usr/local/bin/marshalctl
ENTRYPOINT ["marshalctl"]
