// marshalctl is the CLI: submit, status, cancel, jobs, nodes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AbhiDubz/Marshall/pkg/rpc/marshalpb"
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: marshalctl [-server addr] <command> [flags]

commands:
  submit --cmd "sleep 10" [--cpu 1000] [--mem-gb 1] [--priority 0] [--nodes 1] [--est 5m] [--user $USER]
  status <job-id>
  cancel <job-id>
  jobs   [--state PENDING,RUNNING,...]
  nodes
`)
	os.Exit(2)
}

func main() {
	server := flag.String("server", envOr("MARSHAL_SERVER", "localhost:7070"), "marshald address")
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}

	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal("dial %s: %v", *server, err)
	}
	defer conn.Close()
	cp := marshalpb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch cmd := flag.Arg(0); cmd {
	case "submit":
		fs := flag.NewFlagSet("submit", flag.ExitOnError)
		cmdStr := fs.String("cmd", "", "shell command to run (required)")
		cpu := fs.Int64("cpu", 1000, "CPU request per node, millicores")
		memGB := fs.Int64("mem-gb", 1, "memory request per node, GiB")
		prio := fs.Int("priority", 0, "priority (higher runs first)")
		nodes := fs.Int("nodes", 1, "node count (>1 = gang, all-or-nothing)")
		est := fs.Duration("est", 5*time.Minute, "estimated runtime")
		user := fs.String("user", envOr("USER", "unknown"), "submitting user")
		_ = fs.Parse(flag.Args()[1:])
		if *cmdStr == "" {
			fatal("--cmd is required")
		}
		resp, err := cp.SubmitJob(ctx, &marshalpb.SubmitJobRequest{Spec: &marshalpb.JobSpec{
			User:     *user,
			Priority: int32(*prio),
			Request: &marshalpb.ResourceSpec{
				CpuMillis:   *cpu,
				MemoryBytes: *memGB << 30,
			},
			NodeCount:    int32(*nodes),
			EstRuntimeMs: est.Milliseconds(),
			Cmd:          *cmdStr,
		}})
		if err != nil {
			fatal("submit: %v", err)
		}
		fmt.Println(resp.GetJobId())

	case "status":
		if flag.NArg() < 2 {
			usage()
		}
		info, err := cp.GetJob(ctx, &marshalpb.GetJobRequest{JobId: flag.Arg(1)})
		if err != nil {
			fatal("status: %v", err)
		}
		printJobs(info)

	case "cancel":
		if flag.NArg() < 2 {
			usage()
		}
		resp, err := cp.CancelJob(ctx, &marshalpb.CancelJobRequest{JobId: flag.Arg(1)})
		if err != nil {
			fatal("cancel: %v", err)
		}
		fmt.Printf("%s: %s\n", flag.Arg(1), resp.GetState())

	case "jobs":
		fs := flag.NewFlagSet("jobs", flag.ExitOnError)
		state := fs.String("state", "", "comma-separated state filter")
		_ = fs.Parse(flag.Args()[1:])
		req := &marshalpb.ListJobsRequest{}
		if *state != "" {
			for _, s := range splitComma(*state) {
				req.States = append(req.States, s)
			}
		}
		resp, err := cp.ListJobs(ctx, req)
		if err != nil {
			fatal("jobs: %v", err)
		}
		printJobs(resp.GetJobs()...)

	case "nodes":
		resp, err := cp.ListNodes(ctx, &marshalpb.ListNodesRequest{})
		if err != nil {
			fatal("nodes: %v", err)
		}
		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NODE\tSTATE\tCPU(USED/CAP)\tMEM(USED/CAP)\tLAST-HEARTBEAT")
		for _, n := range resp.GetNodes() {
			state := "healthy"
			if n.GetDraining() {
				state = "draining"
			} else if !n.GetHealthy() {
				state = "DEAD"
			}
			fmt.Fprintf(w, "%s\t%s\t%dm/%dm\t%s/%s\t%s\n",
				n.GetId(), state,
				n.GetAllocated().GetCpuMillis(), n.GetCapacity().GetCpuMillis(),
				gb(n.GetAllocated().GetMemoryBytes()), gb(n.GetCapacity().GetMemoryBytes()),
				time.UnixMilli(n.GetLastHeartbeatUnixMs()).UTC().Format(time.RFC3339))
		}
		w.Flush()

	default:
		usage()
	}
}

func printJobs(jobs ...*marshalpb.JobInfo) {
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "JOB\tSTATE\tPRIO\tATTEMPT\tUSER\tNODES\tSUBMITTED\tCMD")
	for _, j := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%v\t%s\t%s\n",
			j.GetId(), j.GetState(), j.GetSpec().GetPriority(), j.GetAttempt(),
			j.GetSpec().GetUser(), j.GetNodeIds(),
			time.UnixMilli(j.GetSubmitAtUnixMs()).UTC().Format(time.RFC3339),
			j.GetSpec().GetCmd())
	}
	w.Flush()
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func gb(b int64) string { return fmt.Sprintf("%.1fG", float64(b)/(1<<30)) }

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "marshalctl: "+format+"\n", args...)
	os.Exit(1)
}
