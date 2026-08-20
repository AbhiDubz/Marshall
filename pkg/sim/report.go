package sim

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/AbhiDubz/Marshall/pkg/types"
)

// Result is one simulation run's measurements. Every number is
// computed from the run itself — nothing here is ever estimated or
// fabricated — and String() renders deterministically (fixed field
// order, fixed float precision), so identical runs are byte-identical.
type Result struct {
	Trace     string
	Scheduler string
	Allocator string

	Jobs          int
	Completed     int
	Unschedulable int

	MakespanMS     int64
	UtilizationCPU float64 // fraction of cluster CPU-time in use over the makespan
	UtilizationMem float64

	Waits map[int]*WaitStats // by priority
}

// WaitStats aggregates queue wait (submit -> start) for one priority.
type WaitStats struct {
	Count  int
	MeanMS int64
	P95MS  int64
}

func (e *Engine) result(unschedulable int) *Result {
	r := &Result{
		Trace:         e.trace.Name,
		Scheduler:     e.scheduler.Name(),
		Jobs:          len(e.trace.Jobs),
		Completed:     e.completed,
		Unschedulable: unschedulable,
		Waits:         make(map[int]*WaitStats),
	}
	if !e.lastFinish.IsZero() {
		r.MakespanMS = e.lastFinish.Sub(e.firstSubmit).Milliseconds()
	}

	var totalCPU, totalMemMiB int64
	for _, n := range e.nodes {
		totalCPU += n.Capacity.CPUMillis
		totalMemMiB += n.Capacity.MemoryBytes >> 20
	}
	if r.MakespanMS > 0 {
		r.UtilizationCPU = e.cpuMSAcc / (float64(totalCPU) * float64(r.MakespanMS))
		r.UtilizationMem = e.memMSAcc / (float64(totalMemMiB) * float64(r.MakespanMS))
	}

	byPrio := make(map[int][]int64)
	for _, sj := range e.jobs {
		if sj.job.State != types.Completed && sj.job.State != types.Running {
			continue
		}
		byPrio[sj.job.Priority] = append(byPrio[sj.job.Priority], sj.waited.Milliseconds())
	}
	for p, waits := range byPrio {
		sort.Slice(waits, func(i, k int) bool { return waits[i] < waits[k] })
		var sum int64
		for _, w := range waits {
			sum += w
		}
		idx := (len(waits)*95 + 99) / 100 // ceil(0.95n)
		if idx > 0 {
			idx--
		}
		r.Waits[p] = &WaitStats{
			Count:  len(waits),
			MeanMS: sum / int64(len(waits)),
			P95MS:  waits[idx],
		}
	}
	return r
}

// String renders the canonical report. Field order and precision are
// fixed: running the same trace twice must produce byte-identical
// output.
func (r *Result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "trace=%s sched=%s alloc=%s\n", r.Trace, r.Scheduler, r.Allocator)
	fmt.Fprintf(&b, "jobs=%d completed=%d unschedulable=%d\n", r.Jobs, r.Completed, r.Unschedulable)
	fmt.Fprintf(&b, "makespan=%s\n", fmtDur(r.MakespanMS))
	fmt.Fprintf(&b, "utilization cpu=%.4f mem=%.4f\n", r.UtilizationCPU, r.UtilizationMem)
	prios := make([]int, 0, len(r.Waits))
	for p := range r.Waits {
		prios = append(prios, p)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(prios)))
	for _, p := range prios {
		w := r.Waits[p]
		fmt.Fprintf(&b, "wait prio=%d n=%d mean=%s p95=%s\n", p, w.Count, fmtDur(w.MeanMS), fmtDur(w.P95MS))
	}
	return b.String()
}

func fmtDur(ms int64) string {
	return time.Duration(ms * int64(time.Millisecond)).Truncate(time.Millisecond).String()
}

// CompareTable renders results as an aligned utilization/wait
// comparison. Rows are printed in the given order; entries with a nil
// Result render as "NOT IMPLEMENTED" (a stubbed policy is an honest
// gap, never a fabricated number).
type CompareRow struct {
	Trace, Scheduler, Allocator string
	Result                      *Result // nil => policy panicked (stub)
}

func CompareTable(rows []CompareRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-10s %-10s %-10s %10s %10s %14s %14s\n",
		"TRACE", "SCHED", "ALLOC", "UTIL-CPU", "MAKESPAN", "MEAN-WAIT", "P95-WAIT")
	for _, row := range rows {
		if row.Result == nil {
			fmt.Fprintf(&b, "%-10s %-10s %-10s %10s %10s %14s %14s\n",
				row.Trace, row.Scheduler, row.Allocator, "-", "-", "NOT IMPL", "-")
			continue
		}
		r := row.Result
		mean, p95 := overallWait(r)
		fmt.Fprintf(&b, "%-10s %-10s %-10s %10.4f %10s %14s %14s\n",
			row.Trace, row.Scheduler, row.Allocator,
			r.UtilizationCPU, fmtDur(r.MakespanMS), fmtDur(mean), fmtDur(p95))
	}
	return b.String()
}

// overallWait pools wait stats across priorities (weighted mean; max
// of per-priority p95s as a conservative pooled p95).
func overallWait(r *Result) (mean, p95 int64) {
	var sum, n int64
	for _, w := range r.Waits {
		sum += w.MeanMS * int64(w.Count)
		n += int64(w.Count)
		if w.P95MS > p95 {
			p95 = w.P95MS
		}
	}
	if n > 0 {
		mean = sum / n
	}
	return mean, p95
}

// CompareChartSVG renders a grouped bar chart of CPU utilization per
// (trace, scheduler, allocator) combination as a self-contained SVG.
// Missing (stubbed) entries render as hatched empty slots.
func CompareChartSVG(rows []CompareRow) string {
	const barW, gap, chartH, padL, padB, padT = 26, 10, 260, 50, 70, 30
	width := padL + len(rows)*(barW+gap) + 20
	height := chartH + padB + padT

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" font-family="monospace" font-size="10">`, width, height)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="white"/>`, width, height)
	// Y axis: 0..1 utilization.
	for _, tick := range []float64{0, 0.25, 0.5, 0.75, 1.0} {
		y := float64(padT) + float64(chartH)*(1-tick)
		fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#ddd"/>`, padL, y, width-10, y)
		fmt.Fprintf(&b, `<text x="%d" y="%.1f" text-anchor="end">%.2f</text>`, padL-4, y+3, tick)
	}
	colors := map[string]string{"fifo": "#4477aa", "backfill": "#66ccee", "gang": "#228833"}
	for i, row := range rows {
		x := padL + i*(barW+gap)
		label := fmt.Sprintf("%s/%s/%s", row.Trace, row.Scheduler, row.Allocator)
		if row.Result == nil {
			fmt.Fprintf(&b, `<rect x="%d" y="%d" width="%d" height="%d" fill="none" stroke="#bbb" stroke-dasharray="3,2"/>`,
				x, padT, barW, chartH)
		} else {
			u := row.Result.UtilizationCPU
			h := float64(chartH) * u
			color, ok := colors[row.Scheduler]
			if !ok {
				color = "#999"
			}
			fmt.Fprintf(&b, `<rect x="%d" y="%.1f" width="%d" height="%.1f" fill="%s"/>`,
				x, float64(padT)+float64(chartH)-h, barW, h, color)
			fmt.Fprintf(&b, `<text x="%d" y="%.1f" text-anchor="middle">%.2f</text>`,
				x+barW/2, float64(padT)+float64(chartH)-h-4, u)
		}
		fmt.Fprintf(&b, `<text x="%d" y="%d" text-anchor="end" transform="rotate(-55 %d %d)">%s</text>`,
			x+barW/2, height-8, x+barW/2, height-8, label)
	}
	b.WriteString(`</svg>`)
	return b.String()
}
