// Package metrics is marshal's Prometheus instrumentation. One
// Registry owns every collector; the control plane updates it from
// the scheduling loop and RPC handlers, and serves it over /metrics.
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Registry holds marshal's collectors.
type Registry struct {
	reg *prometheus.Registry

	QueueDepth      *prometheus.GaugeVec // by state
	NodeUtilization *prometheus.GaugeVec // by node, resource (0..1)
	NodesHealthy    prometheus.Gauge
	WaitSeconds     *prometheus.HistogramVec // by priority: submit -> start
	Preemptions     prometheus.Counter
	Dispatches      *prometheus.CounterVec // by outcome: ok | requeued
	JobsFinished    *prometheus.CounterVec // by final state
	ScheduleCycles  prometheus.Counter
}

// New builds a Registry with all collectors registered.
func New() *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{
		reg: reg,
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "marshal_queue_depth",
			Help: "Jobs per state.",
		}, []string{"state"}),
		NodeUtilization: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "marshal_node_utilization_ratio",
			Help: "Allocated/capacity per node and resource (0..1).",
		}, []string{"node", "resource"}),
		NodesHealthy: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "marshal_nodes_healthy",
			Help: "Nodes currently heartbeating within the timeout.",
		}),
		WaitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "marshal_job_wait_seconds",
			Help:    "Queue wait (submit to start) by priority.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14), // 1s .. ~2.3h
		}, []string{"priority"}),
		Preemptions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "marshal_preemptions_total",
			Help: "Jobs preempted to make room for higher priority work.",
		}),
		Dispatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "marshal_dispatches_total",
			Help: "Dispatch attempts by outcome.",
		}, []string{"outcome"}),
		JobsFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "marshal_jobs_finished_total",
			Help: "Jobs reaching a terminal report, by state.",
		}, []string{"state"}),
		ScheduleCycles: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "marshal_schedule_cycles_total",
			Help: "Scheduler loop iterations.",
		}),
	}
	reg.MustRegister(r.QueueDepth, r.NodeUtilization, r.NodesHealthy, r.WaitSeconds,
		r.Preemptions, r.Dispatches, r.JobsFinished, r.ScheduleCycles)
	return r
}

// Handler serves the registry in Prometheus exposition format.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// SnapshotJobs sets queue depths from a state count map.
func (r *Registry) SnapshotJobs(counts map[types.JobState]int) {
	for _, s := range []types.JobState{types.Pending, types.Scheduled, types.Running,
		types.Completed, types.Failed, types.Preempted} {
		r.QueueDepth.WithLabelValues(string(s)).Set(float64(counts[s]))
	}
}

// SnapshotNodes sets per-node utilization and the healthy count.
func (r *Registry) SnapshotNodes(nodes []types.Node, healthy int) {
	for _, n := range nodes {
		cpu, mem := 0.0, 0.0
		if n.Capacity.CPUMillis > 0 {
			cpu = float64(n.Allocated.CPUMillis) / float64(n.Capacity.CPUMillis)
		}
		if n.Capacity.MemoryBytes > 0 {
			mem = float64(n.Allocated.MemoryBytes) / float64(n.Capacity.MemoryBytes)
		}
		r.NodeUtilization.WithLabelValues(n.ID, "cpu").Set(cpu)
		r.NodeUtilization.WithLabelValues(n.ID, "memory").Set(mem)
	}
	r.NodesHealthy.Set(float64(healthy))
}

// ObserveWait records a job's queue wait at dispatch time.
func (r *Registry) ObserveWait(priority int, seconds float64) {
	r.WaitSeconds.WithLabelValues(strconv.Itoa(priority)).Observe(seconds)
}
