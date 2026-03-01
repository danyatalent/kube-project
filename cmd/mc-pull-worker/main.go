package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"
)

type Task struct {
	RunID    string  `json:"run_id"`
	TaskID   int     `json:"task_id"`
	Seed     int64   `json:"seed"`
	Samples  int64   `json:"samples"`
	TaskType string  `json:"task_type"`
	Dim      int     `json:"dim"`
	A        float64 `json:"a"`
	B        float64 `json:"b"`
	Expr     string  `json:"expr"`
}

type TaskResp struct {
	OK   bool  `json:"ok"`
	Done bool  `json:"done"`
	Task *Task `json:"task"`
}

type ResultReq struct {
	RunID      string          `json:"run_id"`
	WorkerID   string          `json:"worker_id"`
	Samples    int64           `json:"samples"`
	DurationMS int64           `json:"duration_ms"`
	Line       json.RawMessage `json:"line"`
}

func main() {
	var (
		dispatcher = flag.String("dispatcher", "http://mc-dispatcher:8080", "dispatcher base url")
		workerID   = flag.String("mc-worker-id", "", "mc-worker id (optional)")
		pollMS     = flag.Int("poll-ms", 300, "poll interval when no tasks")
		oneshotBin = flag.String("oneshot-bin", "/mc-mc-worker", "path to mc-mc-worker")
	)
	flag.Parse()

	if *workerID == "" {
		hn, _ := os.Hostname()
		*workerID = hn
	}

	client := &http.Client{Timeout: 120 * time.Second}

	for {
		task, done, err := fetchTask(client, *dispatcher, *workerID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fetchTask error: %v\n", err)
			time.Sleep(time.Duration(*pollMS) * time.Millisecond)
			continue
		}
		if done {
			// IMPORTANT: do NOT exit — stay resident, wait for next run.
			time.Sleep(time.Duration(*pollMS) * time.Millisecond)
			continue
		}

		line, durMS, err := runOneShot(*oneshotBin, task)
		if err != nil {
			fmt.Fprintf(os.Stderr, "oneshot error: %v\n", err)
			time.Sleep(time.Duration(*pollMS) * time.Millisecond)
			continue
		}

		if err := postResult(client, *dispatcher, *workerID, task.RunID, task.Samples, durMS, line); err != nil {
			fmt.Fprintf(os.Stderr, "postResult error: %v\n", err)
			time.Sleep(time.Duration(*pollMS) * time.Millisecond)
			continue
		}
	}
}

func fetchTask(c *http.Client, base, wid string) (*Task, bool, error) {
	req, _ := http.NewRequest("GET", base+"/task?wid="+wid, nil)
	resp, err := c.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	var tr TaskResp
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, false, err
	}
	if tr.Done || tr.Task == nil {
		return nil, true, nil
	}
	return tr.Task, false, nil
}

func runOneShot(bin string, t *Task) (json.RawMessage, int64, error) {
	args := []string{
		"--type=" + t.TaskType,
		fmt.Sprintf("--samples=%d", t.Samples),
		fmt.Sprintf("--seed=%d", t.Seed),
		fmt.Sprintf("--task=%d", t.TaskID),
	}
	if t.TaskType == "integral" {
		args = append(args,
			fmt.Sprintf("--dim=%d", t.Dim),
			fmt.Sprintf("--a=%g", t.A),
			fmt.Sprintf("--b=%g", t.B),
			"--expr="+t.Expr,
		)
	}

	start := time.Now()
	cmd := exec.Command(bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, 0, err
	}
	durMS := time.Since(start).Milliseconds()

	raw := bytes.TrimSpace(out.Bytes())
	return json.RawMessage(raw), durMS, nil
}

func postResult(c *http.Client, base, workerID, runID string, samples, durMS int64, line json.RawMessage) error {
	body, _ := json.Marshal(ResultReq{
		RunID:      runID,
		WorkerID:   workerID,
		Samples:    samples,
		DurationMS: durMS,
		Line:       line,
	})
	req, _ := http.NewRequest("POST", base+"/result", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("dispatcher returned %s", resp.Status)
	}
	return nil
}
