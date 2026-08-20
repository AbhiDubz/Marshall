package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

func TestRegistryExposesAllMetrics(t *testing.T) {
	r := New()
	r.SnapshotJobs(map[types.JobState]int{types.Pending: 3, types.Running: 2})
	r.SnapshotNodes([]types.Node{{
		ID:        "n1",
		Capacity:  types.ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30},
		Allocated: types.ResourceSpec{CPUMillis: 1000, MemoryBytes: 2 << 30},
	}}, 1)
	r.ObserveWait(5, 12.5)
	r.Preemptions.Inc()
	r.Dispatches.WithLabelValues("ok").Inc()
	r.JobsFinished.WithLabelValues("COMPLETED").Inc()
	r.ScheduleCycles.Inc()

	srv := httptest.NewServer(r.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, want := range []string{
		`marshal_queue_depth{state="PENDING"} 3`,
		`marshal_queue_depth{state="RUNNING"} 2`,
		`marshal_node_utilization_ratio{node="n1",resource="cpu"} 0.25`,
		`marshal_nodes_healthy 1`,
		`marshal_job_wait_seconds_count{priority="5"} 1`,
		`marshal_preemptions_total 1`,
		`marshal_dispatches_total{outcome="ok"} 1`,
		`marshal_jobs_finished_total{state="COMPLETED"} 1`,
		`marshal_schedule_cycles_total 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition missing %q", want)
		}
	}
}
