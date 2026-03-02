// mc-dispatcher.go (node-aware maxChunk cap)
// Drop-in replacement for your current file.
// v.0.5

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"
)

type Mode string

const (
	ModeFixed    Mode = "fixed"
	ModeAdaptive Mode = "adaptive"
)

type RunRequest struct {
	Mode         Mode   `json:"mode"`          // "fixed" | "adaptive"
	TaskType     string `json:"task_type"`     // "pi" | "integral"
	TotalSamples int64  `json:"total_samples"` // W
	BaseSeed     int64  `json:"base_seed"`

	// Fixed mode:
	ChunkSamples int64 `json:"chunk_samples,omitempty"` // g

	// Adaptive mode:
	MinChunk int64   `json:"min_chunk,omitempty"` // g_min
	MaxChunk int64   `json:"max_chunk,omitempty"` // g_max
	Beta     float64 `json:"beta,omitempty"`      // aggressiveness (0..1)
	Alpha    float64 `json:"alpha,omitempty"`     // EMA smoothing (0..1)

	// integral params
	Dim  int     `json:"dim"`
	A    float64 `json:"a"`
	B    float64 `json:"b"`
	Expr string  `json:"expr"`
}

type Task struct {
	RunID    string  `json:"run_id"`
	TaskID   int     `json:"task_id"`
	Seed     int64   `json:"seed"`
	Samples  int64   `json:"samples"`
	TaskType string  `json:"task_type"`
	Dim      int     `json:"dim,omitempty"`
	A        float64 `json:"a,omitempty"`
	B        float64 `json:"b,omitempty"`
	Expr     string  `json:"expr,omitempty"`
}

type TaskResp struct {
	OK   bool  `json:"ok"`
	Done bool  `json:"done"`
	Task *Task `json:"task,omitempty"`
}

type ResultReq struct {
	RunID      string          `json:"run_id"`
	WorkerID   string          `json:"worker_id"`
	Samples    int64           `json:"samples"`
	DurationMS int64           `json:"duration_ms"`
	Line       json.RawMessage `json:"line"`
}

type NodeDebug struct {
	Node   string  `json:"node"`
	N      int64   `json:"n"`
	MeanMS float64 `json:"mean_ms"`
	StdMS  float64 `json:"std_ms"`
	Cap    int64   `json:"cap"`
	Slow   bool    `json:"slow"`
}

type StatusResp struct {
	RunID           string      `json:"run_id"`
	Mode            Mode        `json:"mode"`
	Running         bool        `json:"running"`
	TotalSamples    int64       `json:"total_samples"`
	AssignedSamples int64       `json:"assigned_samples"`
	DoneSamples     int64       `json:"done_samples"`
	ResultsLines    int         `json:"results_lines"`
	StartTime       string      `json:"start_time,omitempty"`
	EndTime         string      `json:"end_time,omitempty"`
	WallMS          int64       `json:"wall_ms,omitempty"`
	NodeDebug       []NodeDebug `json:"node_debug,omitempty"`
}

type taskState struct {
	Task     Task
	Issued   int
	Done     bool
	IssuedAt time.Time
}

type durStats struct {
	n    int64
	mean float64
	m2   float64
}

func (s *durStats) add(x float64) {
	s.n++
	delta := x - s.mean
	s.mean += delta / float64(s.n)
	delta2 := x - s.mean
	s.m2 += delta * delta2
}

func (s *durStats) stddev() float64 {
	if s.n < 2 {
		return 0
	}
	return math.Sqrt(s.m2 / float64(s.n-1))
}

type lineMeta struct {
	TaskID *int64  `json:"task_id"`
	Node   *string `json:"node"`
	Pod    *string `json:"pod"`
}

type Dispatcher struct {
	mu sync.Mutex

	running bool
	runID   string
	req     RunRequest
	start   time.Time
	end     time.Time

	// work accounting
	totalW   int64
	remainW  int64
	assigned int64
	doneW    int64
	taskSeq  int

	// adaptive stats
	speedHat map[string]float64 // samples/sec estimate per worker

	// per-task tracking (for speculative)
	tasks map[int]*taskState

	// duration stats for speculative threshold (global)
	ds durStats

	// --- node-aware stats ---
	workerNode map[string]string    // workerID -> node
	nodeStats  map[string]*durStats // node -> duration stats (ms)
	nodeCap    map[string]int64     // node -> maxChunk cap (samples)
	nodeSlow   map[string]bool      // node -> flagged slow/noisy

	// store JSONL results
	results []json.RawMessage
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		speedHat:   make(map[string]float64),
		tasks:      make(map[int]*taskState),
		workerNode: make(map[string]string),
		nodeStats:  make(map[string]*durStats),
		nodeCap:    make(map[string]int64),
		nodeSlow:   make(map[string]bool),
	}
}

func (d *Dispatcher) validateRun(req *RunRequest) error {
	if req.TaskType != "pi" && req.TaskType != "integral" {
		return fmt.Errorf("task_type must be pi|integral")
	}
	if req.TotalSamples <= 0 {
		return fmt.Errorf("total_samples must be > 0")
	}
	if req.BaseSeed <= 0 {
		req.BaseSeed = 1
	}
	if req.Mode != ModeFixed && req.Mode != ModeAdaptive {
		return fmt.Errorf("mode must be fixed|adaptive")
	}
	if req.TaskType == "integral" {
		if req.Dim <= 0 {
			return fmt.Errorf("dim must be > 0")
		}
		if req.Expr == "" {
			return fmt.Errorf("expr must be non-empty")
		}
	}
	if req.Mode == ModeFixed {
		if req.ChunkSamples <= 0 {
			return fmt.Errorf("chunk_samples must be > 0 for fixed mode")
		}
	}
	if req.Mode == ModeAdaptive {
		if req.MinChunk <= 0 || req.MaxChunk <= 0 || req.MinChunk > req.MaxChunk {
			return fmt.Errorf("min_chunk/max_chunk invalid")
		}
		if req.Beta <= 0 || req.Beta > 1 {
			return fmt.Errorf("beta must be in (0,1]")
		}
		if req.Alpha <= 0 || req.Alpha > 1 {
			return fmt.Errorf("alpha must be in (0,1]")
		}
	}
	return nil
}

func (d *Dispatcher) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req RunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := d.validateRun(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.req = req
	d.runID = fmt.Sprintf("run-%d", time.Now().UnixNano())
	d.running = true
	d.start = time.Now()
	d.end = time.Time{}
	d.totalW = req.TotalSamples
	d.remainW = req.TotalSamples
	d.assigned = 0
	d.doneW = 0
	d.taskSeq = 0
	d.results = d.results[:0]

	d.speedHat = make(map[string]float64)
	d.tasks = make(map[int]*taskState)
	d.ds = durStats{}

	d.workerNode = make(map[string]string)
	d.nodeStats = make(map[string]*durStats)
	d.nodeCap = make(map[string]int64)
	d.nodeSlow = make(map[string]bool)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":     true,
		"run_id": d.runID,
	})
}

func clipInt64(x, lo, hi int64) int64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// tail-aware cap for max chunk (internal, no user params)
func (d *Dispatcher) tailMaxChunkLocked() int64 {
	if d.req.Mode != ModeAdaptive {
		return math.MaxInt64
	}
	// 10% remaining -> cap to max/2, 2% remaining -> cap to max/4
	maxC := d.req.MaxChunk
	if d.totalW <= 0 {
		return maxC
	}
	r := float64(d.remainW) / float64(d.totalW)
	if r <= 0.02 {
		c := maxC / 4
		if c < d.req.MinChunk {
			c = d.req.MinChunk
		}
		return c
	}
	if r <= 0.10 {
		c := maxC / 2
		if c < d.req.MinChunk {
			c = d.req.MinChunk
		}
		return c
	}
	return maxC
}

// recompute node caps based on observed durations.
// This is intentionally simple and fully automatic:
// if a node's mean duration is significantly higher than global mean -> cap its max chunk to max/2.
//
// Parameters (internal):
// - minN: minimum samples for node to be considered
// - gamma: slow threshold multiplier vs global mean
func (d *Dispatcher) recomputeNodeCapsLocked() {
	if d.req.Mode != ModeAdaptive {
		return
	}

	const (
		minN  int64   = 8
		gamma float64 = 1.30
	)

	// compute global mean over nodes with enough samples
	var sumMeans float64
	var cnt int64
	for _, st := range d.nodeStats {
		if st == nil || st.n < minN {
			continue
		}
		sumMeans += st.mean
		cnt++
	}
	if cnt == 0 {
		return
	}
	globalMean := sumMeans / float64(cnt)

	// define default cap
	defCap := d.req.MaxChunk
	halfCap := d.req.MaxChunk / 2
	if halfCap < d.req.MinChunk {
		halfCap = d.req.MinChunk
	}

	for node, st := range d.nodeStats {
		if st == nil || st.n < minN {
			// not enough data -> do not restrict
			d.nodeCap[node] = defCap
			d.nodeSlow[node] = false
			continue
		}
		slow := st.mean > gamma*globalMean
		if slow {
			d.nodeCap[node] = halfCap
			d.nodeSlow[node] = true
		} else {
			d.nodeCap[node] = defCap
			d.nodeSlow[node] = false
		}
	}
}

// effective per-node cap (if node unknown -> unlimited w.r.t. node cap)
func (d *Dispatcher) nodeMaxChunkLocked(workerID string) int64 {
	node, ok := d.workerNode[workerID]
	if !ok || node == "" {
		return math.MaxInt64
	}
	if capN, ok := d.nodeCap[node]; ok && capN > 0 {
		return capN
	}
	// if we know the node but cap is not computed yet -> default max
	return d.req.MaxChunk
}

func (d *Dispatcher) chooseChunkLocked(workerID string) int64 {
	// called with mu held, remainW > 0
	if d.req.Mode == ModeFixed {
		g := d.req.ChunkSamples
		if g > d.remainW {
			g = d.remainW
		}
		return g
	}

	// adaptive
	// reasonable prior (samples/sec). 1.0 is meaningless.
	const priorSpeed = 50_000_000.0
	if _, ok := d.speedHat[workerID]; !ok {
		d.speedHat[workerID] = priorSpeed
		// warmup: first task small to measure speed quickly
		g := minInt64(d.req.MinChunk, d.remainW)
		return g
	}

	// sum speeds
	var sum float64
	for _, v := range d.speedHat {
		sum += v
	}
	if sum <= 0 {
		sum = 1
	}

	// base formula: g = beta * R * (v_i / sum_v)
	gf := d.req.Beta * float64(d.remainW) * (d.speedHat[workerID] / sum)
	g := int64(math.Round(gf))

	// caps: global max, tail max, and node-aware max
	maxTail := d.tailMaxChunkLocked()
	maxEff := minInt64(d.req.MaxChunk, maxTail)

	// node-aware cap (new)
	maxEff = minInt64(maxEff, d.nodeMaxChunkLocked(workerID))

	g = clipInt64(g, d.req.MinChunk, maxEff)

	if g > d.remainW {
		g = d.remainW
	}
	if g <= 0 {
		g = minInt64(d.req.MinChunk, d.remainW)
	}
	return g
}

func (d *Dispatcher) makeTaskLocked(taskID int, samples int64) Task {
	return Task{
		RunID:    d.runID,
		TaskID:   taskID,
		Seed:     d.req.BaseSeed + int64(taskID),
		Samples:  samples,
		TaskType: d.req.TaskType,
		Dim:      d.req.Dim,
		A:        d.req.A,
		B:        d.req.B,
		Expr:     d.req.Expr,
	}
}

func (d *Dispatcher) speculativeCandidateLocked(now time.Time) *taskState {
	// Only consider speculative when:
	// - all work has been assigned (remainW <= 0)
	// - and we still have pending tasks
	if d.remainW > 0 {
		return nil
	}
	if d.doneW >= d.totalW {
		return nil
	}

	mean := d.ds.mean
	std := d.ds.stddev()

	// fallback if little data
	if d.ds.n < 10 {
		mean = 15_000 // ms
		std = 5_000
	}
	thr := mean + 2.0*std
	if thr < 20_000 {
		thr = 20_000
	}

	// pick oldest slow pending task that was issued once
	var best *taskState
	var bestAge time.Duration
	for _, ts := range d.tasks {
		if ts.Done {
			continue
		}
		if ts.Issued < 1 || ts.Issued >= 2 {
			continue
		}
		age := now.Sub(ts.IssuedAt)
		if float64(age.Milliseconds()) < thr {
			continue
		}
		if best == nil || age > bestAge {
			best = ts
			bestAge = age
		}
	}
	return best
}

func (d *Dispatcher) handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	workerID := r.URL.Query().Get("wid")
	if workerID == "" {
		workerID = "unknown"
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TaskResp{OK: true, Done: true})
		return
	}

	// normal assignment if there is remaining work
	if d.remainW > 0 {
		g := d.chooseChunkLocked(workerID)
		taskID := d.taskSeq
		d.taskSeq++

		d.remainW -= g
		d.assigned += g

		t := d.makeTaskLocked(taskID, g)
		d.tasks[taskID] = &taskState{
			Task:     t,
			Issued:   1,
			Done:     false,
			IssuedAt: time.Now(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TaskResp{OK: true, Done: false, Task: &t})
		return
	}

	// No remaining work to assign. Try speculative execution to reduce tail.
	now := time.Now()
	if cand := d.speculativeCandidateLocked(now); cand != nil {
		cand.Issued++
		// re-issue the SAME task to another worker
		cand.IssuedAt = now

		t := cand.Task
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TaskResp{OK: true, Done: false, Task: &t})
		return
	}

	// otherwise: nothing to do, but keep workers alive
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(TaskResp{OK: true, Done: true})
}

func parseLineMeta(line json.RawMessage) (taskID int64, node string, pod string) {
	taskID = -1
	var lm lineMeta
	if err := json.Unmarshal(line, &lm); err != nil {
		return taskID, "", ""
	}
	if lm.TaskID != nil {
		taskID = *lm.TaskID
	}
	if lm.Node != nil {
		node = *lm.Node
	}
	if lm.Pod != nil {
		pod = *lm.Pod
	}
	return taskID, node, pod
}

func (d *Dispatcher) handleResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req ResultReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running || req.RunID != d.runID {
		http.Error(w, "no such running run_id", http.StatusConflict)
		return
	}

	taskID, node, _ := parseLineMeta(req.Line)

	// remember mapping worker -> node (best-effort)
	if node != "" && req.WorkerID != "" {
		d.workerNode[req.WorkerID] = node
		if _, ok := d.nodeStats[node]; !ok {
			d.nodeStats[node] = &durStats{}
		}
	}

	// If this task already completed (e.g. speculative duplicate), ignore accounting but still accept.
	if taskID >= 0 {
		if ts, ok := d.tasks[int(taskID)]; ok && ts.Done {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "ignored": true})
			return
		}
	}

	// store JSONL line (keep for summary)
	d.results = append(d.results, req.Line)

	// mark task done (best effort)
	if taskID >= 0 {
		if ts, ok := d.tasks[int(taskID)]; ok && !ts.Done {
			ts.Done = true
		}
	}

	d.doneW += req.Samples

	// duration stats (global, for speculative threshold)
	if req.DurationMS > 0 {
		d.ds.add(float64(req.DurationMS))
	}

	// duration stats (node-level, for node-aware caps)
	if node != "" && req.DurationMS > 0 {
		if st := d.nodeStats[node]; st != nil {
			st.add(float64(req.DurationMS))
			// update caps on each new sample (cheap, nodes are few)
			d.recomputeNodeCapsLocked()
		}
	}

	// update speed estimate for adaptive
	if d.req.Mode == ModeAdaptive && req.DurationMS > 0 && req.Samples > 0 {
		tilde := float64(req.Samples) / (float64(req.DurationMS) / 1000.0) // samples/sec
		old := d.speedHat[req.WorkerID]
		if old <= 0 {
			old = tilde
		} else {
			// clamp extreme spikes
			lo := old / 3.0
			hi := old * 3.0
			if tilde < lo {
				tilde = lo
			}
			if tilde > hi {
				tilde = hi
			}
		}
		alpha := d.req.Alpha
		d.speedHat[req.WorkerID] = (1-alpha)*old + alpha*tilde
	}

	// finish condition
	if d.assigned >= d.totalW && d.doneW >= d.totalW {
		d.running = false
		d.end = time.Now()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func (d *Dispatcher) handleStatus(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()

	resp := StatusResp{
		RunID:           d.runID,
		Mode:            d.req.Mode,
		Running:         d.running,
		TotalSamples:    d.totalW,
		AssignedSamples: d.assigned,
		DoneSamples:     d.doneW,
		ResultsLines:    len(d.results),
	}
	if !d.start.IsZero() {
		resp.StartTime = d.start.Format(time.RFC3339)
	}
	if !d.end.IsZero() {
		resp.EndTime = d.end.Format(time.RFC3339)
		resp.WallMS = d.end.Sub(d.start).Milliseconds()
	}

	// expose node debug (helps verify node-aware cap works)
	if d.req.Mode == ModeAdaptive && len(d.nodeStats) > 0 {
		dbg := make([]NodeDebug, 0, len(d.nodeStats))
		for node, st := range d.nodeStats {
			if st == nil {
				continue
			}
			capN := d.req.MaxChunk
			if c, ok := d.nodeCap[node]; ok && c > 0 {
				capN = c
			}
			dbg = append(dbg, NodeDebug{
				Node:   node,
				N:      st.n,
				MeanMS: st.mean,
				StdMS:  st.stddev(),
				Cap:    capN,
				Slow:   d.nodeSlow[node],
			})
		}
		resp.NodeDebug = dbg
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (d *Dispatcher) handleResults(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	bw := bufio.NewWriter(w)
	for _, line := range d.results {
		_, _ = bw.Write(line)
		_, _ = bw.WriteString("\n")
	}
	_ = bw.Flush()
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	d := NewDispatcher()
	mux := http.NewServeMux()
	mux.HandleFunc("/run", d.handleRun)
	mux.HandleFunc("/task", d.handleTask)
	mux.HandleFunc("/result", d.handleResult)
	mux.HandleFunc("/status", d.handleStatus)
	mux.HandleFunc("/results", d.handleResults)

	log.Printf("mc-dispatcher listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
