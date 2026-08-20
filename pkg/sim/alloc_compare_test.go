package sim

import "testing"

func TestBestFitRunsAllTraceKinds(t *testing.T) {
	for _, kind := range []string{"uniform", "bimodal", "bursty"} {
		tr, err := Generate(kind, 42, 120)
		if err != nil {
			t.Fatal(err)
		}
		res := run(t, tr, "fifo", "bestfit")
		if res.Completed == 0 {
			t.Fatalf("%s: nothing completed", kind)
		}
	}
}
