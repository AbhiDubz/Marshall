package types

import (
	"testing"
	"time"
)

func TestResourceSpecArithmetic(t *testing.T) {
	a := ResourceSpec{CPUMillis: 4000, MemoryBytes: 8 << 30, GRES: map[string]int{"gpu": 2}}
	b := ResourceSpec{CPUMillis: 1000, MemoryBytes: 2 << 30, GRES: map[string]int{"gpu": 1}}

	sum := a.Add(b)
	if sum.CPUMillis != 5000 || sum.MemoryBytes != 10<<30 || sum.GRES["gpu"] != 3 {
		t.Fatalf("Add wrong: %+v", sum)
	}
	diff := a.Sub(b)
	if diff.CPUMillis != 3000 || diff.MemoryBytes != 6<<30 || diff.GRES["gpu"] != 1 {
		t.Fatalf("Sub wrong: %+v", diff)
	}
	// Originals untouched.
	if a.CPUMillis != 4000 || a.GRES["gpu"] != 2 || b.GRES["gpu"] != 1 {
		t.Fatalf("Add/Sub mutated inputs: a=%+v b=%+v", a, b)
	}
}

func TestResourceSpecFits(t *testing.T) {
	avail := ResourceSpec{CPUMillis: 2000, MemoryBytes: 4 << 30, GRES: map[string]int{"gpu": 1}}

	cases := []struct {
		name string
		req  ResourceSpec
		want bool
	}{
		{"exact", ResourceSpec{CPUMillis: 2000, MemoryBytes: 4 << 30, GRES: map[string]int{"gpu": 1}}, true},
		{"smaller", ResourceSpec{CPUMillis: 500, MemoryBytes: 1 << 30}, true},
		{"cpu over", ResourceSpec{CPUMillis: 2001, MemoryBytes: 1 << 30}, false},
		{"mem over", ResourceSpec{CPUMillis: 100, MemoryBytes: 5 << 30}, false},
		{"gres over", ResourceSpec{CPUMillis: 100, MemoryBytes: 1 << 30, GRES: map[string]int{"gpu": 2}}, false},
		{"gres kind missing", ResourceSpec{CPUMillis: 100, MemoryBytes: 1 << 30, GRES: map[string]int{"fpga": 1}}, false},
		{"zero", ResourceSpec{}, true},
	}
	for _, c := range cases {
		if got := c.req.Fits(avail); got != c.want {
			t.Errorf("%s: Fits=%v want %v", c.name, got, c.want)
		}
	}
}

func TestCloneIsDeep(t *testing.T) {
	a := ResourceSpec{CPUMillis: 1, GRES: map[string]int{"gpu": 1}}
	b := a.Clone()
	b.GRES["gpu"] = 99
	if a.GRES["gpu"] != 1 {
		t.Fatal("Clone shares GRES map")
	}

	n := Node{ID: "n1", Capacity: a.Clone(), Allocated: ResourceSpec{GRES: map[string]int{"gpu": 0}}}
	nc := n.Clone()
	nc.Capacity.GRES["gpu"] = 42
	if n.Capacity.GRES["gpu"] != 1 {
		t.Fatal("Node.Clone shares capacity GRES map")
	}

	al := Allocation{JobID: "j", NodeIDs: []string{"n1", "n2"}}
	alc := al.Clone()
	alc.NodeIDs[0] = "zzz"
	if al.NodeIDs[0] != "n1" {
		t.Fatal("Allocation.Clone shares NodeIDs slice")
	}
}

func TestNodeAvailableAndCanFit(t *testing.T) {
	n := Node{
		ID:        "n1",
		Capacity:  ResourceSpec{CPUMillis: 8000, MemoryBytes: 16 << 30, GRES: map[string]int{"gpu": 4}},
		Allocated: ResourceSpec{CPUMillis: 6000, MemoryBytes: 8 << 30, GRES: map[string]int{"gpu": 3}},
	}
	avail := n.Available()
	if avail.CPUMillis != 2000 || avail.MemoryBytes != 8<<30 || avail.GRES["gpu"] != 1 {
		t.Fatalf("Available wrong: %+v", avail)
	}
	if !n.CanFit(ResourceSpec{CPUMillis: 2000, GRES: map[string]int{"gpu": 1}}) {
		t.Fatal("expected fit")
	}
	if n.CanFit(ResourceSpec{CPUMillis: 2001}) {
		t.Fatal("expected no fit")
	}
	n.Draining = true
	if n.CanFit(ResourceSpec{CPUMillis: 1}) {
		t.Fatal("draining node must not accept jobs")
	}
}

func TestStateMachine(t *testing.T) {
	legal := [][2]JobState{
		{Pending, Scheduled},
		{Scheduled, Running},
		{Scheduled, Pending},
		{Scheduled, Failed},
		{Running, Completed},
		{Running, Failed},
		{Running, Preempted},
		{Preempted, Pending},
		{Failed, Pending},
	}
	for _, e := range legal {
		if !LegalTransition(e[0], e[1]) {
			t.Errorf("expected legal: %s -> %s", e[0], e[1])
		}
		if err := ValidateTransition(e[0], e[1]); err != nil {
			t.Errorf("ValidateTransition(%s,%s): %v", e[0], e[1], err)
		}
	}
	illegal := [][2]JobState{
		{Completed, Running}, // the brief's canonical example
		{Completed, Pending},
		{Pending, Running},
		{Pending, Completed},
		{Running, Scheduled},
		{Running, Running},
		{Preempted, Running},
		{Failed, Running},
		{Scheduled, Completed},
	}
	for _, e := range illegal {
		if LegalTransition(e[0], e[1]) {
			t.Errorf("expected illegal: %s -> %s", e[0], e[1])
		}
		if err := ValidateTransition(e[0], e[1]); err == nil {
			t.Errorf("ValidateTransition(%s,%s): want error", e[0], e[1])
		}
	}
}

func TestSortJobsFIFO(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := []Job{
		{ID: "c", Priority: 1, SubmitAt: t0.Add(2 * time.Second)},
		{ID: "b", Priority: 5, SubmitAt: t0.Add(3 * time.Second)},
		{ID: "a", Priority: 1, SubmitAt: t0.Add(2 * time.Second)},
		{ID: "d", Priority: 1, SubmitAt: t0},
		{ID: "e", Priority: 5, SubmitAt: t0.Add(1 * time.Second)},
	}
	SortJobsFIFO(jobs)
	want := []string{"e", "b", "d", "a", "c"}
	for i, id := range want {
		if jobs[i].ID != id {
			t.Fatalf("order wrong at %d: got %s want %s (full: %v)", i, jobs[i].ID, id, ids(jobs))
		}
	}
}

func ids(jobs []Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}
