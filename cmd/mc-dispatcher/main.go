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
	ModeFixed    Mode = "fixed"    // Baseline B
	ModeAdaptive Mode = "adaptive" // Proposed
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
	Samples    int64           `json:"samples"`     // chunk size g actually executed
	DurationMS int64           `json:"duration_ms"` // measured by mc-worker
	Line       json.RawMessage `json:"line"`        // raw JSON line from mc-mc-worker
}

type StatusResp struct {
	RunID           string `json:"run_id"`
	Mode            Mode   `json:"mode"`
	Running         bool   `json:"running"`
	TotalSamples    int64  `json:"total_samples"`
	AssignedSamples int64  `json:"assigned_samples"`
	DoneSamples     int64  `json:"done_samples"`
	ResultsLines    int    `json:"results_lines"`
	StartTime       string `json:"start_time,omitempty"`
	EndTime         string `json:"end_time,omitempty"`
	WallMS          int64  `json:"wall_ms,omitempty"`
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

	// fixed mode queue index (optional, but we can just use remainW)
	// adaptive stats
	speedHat map[string]float64 // samples/sec estimate per mc-worker

	// store JSONL results
	results []json.RawMessage
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		speedHat: make(map[string]float64),
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

	// reset state
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

	// reset speed estimates
	d.speedHat = make(map[string]float64)

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
	// ensure mc-worker has initial speed
	if _, ok := d.speedHat[workerID]; !ok {
		d.speedHat[workerID] = 1.0 // neutral initial guess
	}

	// sum speeds
	var sum float64
	for _, v := range d.speedHat {
		sum += v
	}
	if sum <= 0 {
		sum = 1
	}

	// g = beta * R * (s_j / sum_s)
	gf := d.req.Beta * float64(d.remainW) * (d.speedHat[workerID] / sum)
	g := int64(math.Round(gf))

	g = clipInt64(g, d.req.MinChunk, d.req.MaxChunk)
	if g > d.remainW {
		g = d.remainW
	}
	if g <= 0 {
		g = minInt64(d.req.MinChunk, d.remainW)
	}
	return g
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
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

	if !d.running || d.remainW <= 0 {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TaskResp{OK: true, Done: true})
		return
	}

	g := d.chooseChunkLocked(workerID)
	taskID := d.taskSeq
	d.taskSeq++

	d.remainW -= g
	d.assigned += g

	t := Task{
		RunID:    d.runID,
		TaskID:   taskID,
		Seed:     d.req.BaseSeed + int64(taskID),
		Samples:  g,
		TaskType: d.req.TaskType,
		Dim:      d.req.Dim,
		A:        d.req.A,
		B:        d.req.B,
		Expr:     d.req.Expr,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(TaskResp{OK: true, Done: false, Task: &t})
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

	// store JSONL line
	d.results = append(d.results, req.Line)
	d.doneW += req.Samples

	// update speed estimate for adaptive
	if d.req.Mode == ModeAdaptive && req.DurationMS > 0 && req.Samples > 0 {
		tilde := float64(req.Samples) / (float64(req.DurationMS) / 1000.0) // samples/sec
		old := d.speedHat[req.WorkerID]
		if old <= 0 {
			old = tilde
		}
		alpha := d.req.Alpha
		d.speedHat[req.WorkerID] = (1-alpha)*old + alpha*tilde
	}

	// finish condition: all work assigned AND all results returned
	// In practice, doneW reaches totalW if workers return exactly assigned.
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
