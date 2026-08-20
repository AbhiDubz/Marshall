package sim

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/AbhiDubz/Marshall/pkg/alloc"
	"github.com/AbhiDubz/Marshall/pkg/sched"
)

func run(t *testing.T, tr *Trace, schedName, allocName string) *Result {
	t.Helper()
	a, ok := alloc.ByName(allocName)
	if !ok {
		t.Fatalf("unknown allocator %s", allocName)
	}
	e := NewEngine(tr)
	s, ok := sched.ByName(schedName, a, e.Lookup)
	if !ok {
		t.Fatalf("unknown scheduler %s", schedName)
	}
	e.SetScheduler(s)
	res, err := e.Run()
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	res.Allocator = allocName
	return res
}

func TestGeneratorIsDeterministic(t *testing.T) {
	for _, kind := range []string{"uniform", "bimodal", "bursty"} {
		a, err := Generate(kind, 42, 100)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		b, err := Generate(kind, 42, 100)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("%s: same seed produced different traces", kind)
		}
		c, _ := Generate(kind, 43, 100)
		if reflect.DeepEqual(a.Jobs, c.Jobs) {
			t.Fatalf("%s: different seeds produced identical job streams", kind)
		}
	}
}

func TestGeneratorShapes(t *testing.T) {
	bi, err := Generate("bimodal", 7, 300)
	if err != nil {
		t.Fatal(err)
	}
	gang, gres := 0, 0
	for _, j := range bi.Jobs {
		if j.NodeCount > 1 {
			gang++
		}
		if len(j.GRES) > 0 {
			gres++
		}
	}
	if gang == 0 {
		t.Fatal("bimodal trace must contain gang jobs")
	}
	if gres == 0 {
		t.Fatal("bimodal trace must contain GRES jobs")
	}
	if _, err := Generate("nope", 1, 10); err == nil {
		t.Fatal("unknown kind must error")
	}
}

func TestTraceRoundTrip(t *testing.T) {
	tr, err := Generate("uniform", 42, 50)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "trace.json")
	if err := tr.Save(path); err != nil {
		t.Fatal(err)
	}
	back, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tr, back) {
		t.Fatal("trace changed across save/load")
	}
}

func TestEngineTinyTraceExactNumbers(t *testing.T) {
	// One node, two jobs; second must wait for the first.
	tr := &Trace{
		Name:    "tiny",
		Cluster: []NodeGroup{{Count: 1, CPUMillis: 1000, MemoryBytes: 1 << 30}},
		Jobs: []TraceJob{
			{ID: "a", User: "u", Priority: 0, CPUMillis: 1000, MemoryBytes: 1 << 30,
				NodeCount: 1, EstRuntimeMS: 10000, TrueRuntimeMS: 10000, SubmitOffsetMS: 0},
			{ID: "b", User: "u", Priority: 0, CPUMillis: 1000, MemoryBytes: 1 << 30,
				NodeCount: 1, EstRuntimeMS: 10000, TrueRuntimeMS: 5000, SubmitOffsetMS: 1000},
		},
	}
	res := run(t, tr, "fifo", "firstfit")
	if res.Completed != 2 || res.Unschedulable != 0 {
		t.Fatalf("completed=%d unsched=%d", res.Completed, res.Unschedulable)
	}
	// a: 0-10s. b: submits at 1s, starts at 10s (waited 9s), ends 15s.
	if res.MakespanMS != 15000 {
		t.Fatalf("makespan=%dms want 15000", res.MakespanMS)
	}
	w := res.Waits[0]
	if w == nil || w.Count != 2 {
		t.Fatalf("wait stats missing: %+v", res.Waits)
	}
	if w.MeanMS != 4500 || w.P95MS != 9000 {
		t.Fatalf("mean=%d p95=%d want 4500/9000", w.MeanMS, w.P95MS)
	}
	// Utilization: 10s + 5s of 1000m over 15s of 1000m = 1.0.
	if res.UtilizationCPU < 0.999 || res.UtilizationCPU > 1.001 {
		t.Fatalf("utilization=%f want 1.0", res.UtilizationCPU)
	}
}

func TestEngineUnschedulableJobReported(t *testing.T) {
	tr := &Trace{
		Name:    "toobig",
		Cluster: []NodeGroup{{Count: 1, CPUMillis: 1000, MemoryBytes: 1 << 30}},
		Jobs: []TraceJob{
			{ID: "big", User: "u", CPUMillis: 9999, MemoryBytes: 1 << 30,
				NodeCount: 1, EstRuntimeMS: 1000, TrueRuntimeMS: 1000, SubmitOffsetMS: 0},
		},
	}
	res := run(t, tr, "fifo", "firstfit")
	if res.Unschedulable != 1 || res.Completed != 0 {
		t.Fatalf("want 1 unschedulable, got %+v", res)
	}
}

func TestSimulationIsByteIdenticalAcrossRuns(t *testing.T) {
	tr, err := Generate("uniform", 42, 150)
	if err != nil {
		t.Fatal(err)
	}
	out1 := run(t, tr, "fifo", "firstfit").String()
	out2 := run(t, tr, "fifo", "firstfit").String()
	if out1 != out2 {
		t.Fatalf("same trace produced different reports:\n%s\nvs\n%s", out1, out2)
	}
	if out1 == "" {
		t.Fatal("empty report")
	}
}

