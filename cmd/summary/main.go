package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
)

type Result struct {
	TaskType string  `json:"task_type"`
	Func     string  `json:"func,omitempty"`
	Expr     string  `json:"expr,omitempty"`
	A        float64 `json:"a,omitempty"`
	B        float64 `json:"b,omitempty"`
	Dim      int     `json:"dim,omitempty"`

	TaskID     int     `json:"task_id"`
	Samples    int64   `json:"samples"`
	Acc        float64 `json:"acc"`
	Estimate   float64 `json:"estimate"`
	DurationMs int64   `json:"duration_ms"`
	Node       string  `json:"node"`
	Pod        string  `json:"pod"`
}

type NodeAgg struct {
	Node          string
	UniquePods    map[string]struct{}
	Chunks        int
	Samples       int64
	SumDurationMs int64
	MaxDurationMs int64
}

func almostEqual(a, b float64) bool {
	const eps = 1e-12
	return math.Abs(a-b) <= eps*(1+math.Abs(a)+math.Abs(b))
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	var (
		chunks       int
		totalSamples int64
		totalAcc     float64

		sumChunkMs int64
		maxChunkMs int64

		taskType string
		funcName string
		exprStr  string
		aVal     float64
		bVal     float64
		dimVal   int

		warnings []string
	)

	uniquePods := map[string]struct{}{}
	uniqueNodes := map[string]struct{}{}

	byNode := map[string]*NodeAgg{}

	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}

		var r Result
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}

		if chunks == 0 {
			taskType = r.TaskType
			funcName = r.Func
			exprStr = r.Expr
			aVal = r.A
			bVal = r.B
			dimVal = r.Dim
			if dimVal == 0 {
				dimVal = 1
			}
		} else {
			if r.TaskType != taskType {
				warnings = append(warnings, fmt.Sprintf("WARNING: mixed task types: %q and %q", taskType, r.TaskType))
			}
			if taskType == "integral" {
				if !almostEqual(r.A, aVal) {
					warnings = append(warnings, fmt.Sprintf("WARNING: mixed A values: %v vs %v", aVal, r.A))
				}
				if !almostEqual(r.B, bVal) {
					warnings = append(warnings, fmt.Sprintf("WARNING: mixed B values: %v vs %v", bVal, r.B))
				}
				d := r.Dim
				if d == 0 {
					d = 1
				}
				if d != dimVal {
					warnings = append(warnings, fmt.Sprintf("WARNING: mixed dim values: %d vs %d", dimVal, d))
				}
				if exprStr != "" && r.Expr != "" && r.Expr != exprStr {
					warnings = append(warnings, "WARNING: mixed expr values across chunks")
				}
			}
		}

		chunks++
		totalSamples += r.Samples
		totalAcc += r.Acc

		sumChunkMs += r.DurationMs
		if r.DurationMs > maxChunkMs {
			maxChunkMs = r.DurationMs
		}

		if r.Pod != "" {
			uniquePods[r.Pod] = struct{}{}
		}
		if r.Node != "" {
			uniqueNodes[r.Node] = struct{}{}
		}

		na, ok := byNode[r.Node]
		if !ok {
			na = &NodeAgg{Node: r.Node, UniquePods: map[string]struct{}{}}
			byNode[r.Node] = na
		}
		na.Chunks++
		na.Samples += r.Samples
		na.SumDurationMs += r.DurationMs
		if r.DurationMs > na.MaxDurationMs {
			na.MaxDurationMs = r.DurationMs
		}
		if r.Pod != "" {
			na.UniquePods[r.Pod] = struct{}{}
		}
	}

	if err := in.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "scan:", err)
		os.Exit(1)
	}
	if chunks == 0 {
		fmt.Println("No JSON results found on stdin.")
		os.Exit(2)
	}

	fmt.Println("=== Aggregated results (JSONL lines = chunks) ===")
	fmt.Printf("Chunks: %d\n", chunks)
	fmt.Printf("Unique pods: %d\n", len(uniquePods))
	fmt.Printf("Unique nodes: %d\n", len(uniqueNodes))
	fmt.Printf("Task type: %s\n", taskType)

	var finalEstimate float64
	switch taskType {
	case "pi":
		finalEstimate = 4.0 * totalAcc / float64(totalSamples)
		fmt.Printf("Total samples: %d\n", totalSamples)
		fmt.Printf("Pi estimate (aggregated): %.10f\n", finalEstimate)

	case "integral":
		width := bVal - aVal
		if dimVal <= 0 {
			dimVal = 1
		}
		volume := math.Pow(width, float64(dimVal))
		finalEstimate = volume * (totalAcc / float64(totalSamples))

		fmt.Printf("Interval: [%.12g, %.12g]^%d\n", aVal, bVal, dimVal)
		if exprStr != "" {
			fmt.Printf("Expr: %s\n", exprStr)
		} else {
			fmt.Printf("Func: %s\n", funcName)
		}
		fmt.Printf("Total samples: %d\n", totalSamples)
		fmt.Printf("Integral estimate (aggregated): %.10f\n", finalEstimate)

	default:
		fmt.Printf("Total samples: %d\n", totalSamples)
		fmt.Printf("Aggregated estimate: %.10f\n", totalAcc/float64(totalSamples))
	}

	avgChunkMs := float64(sumChunkMs) / float64(chunks)
	fmt.Printf("Avg chunk duration: %.2f ms\n", avgChunkMs)
	fmt.Printf("Max chunk duration (straggler): %d ms\n", maxChunkMs)

	if len(warnings) > 0 {
		fmt.Println()
		for _, w := range warnings {
			fmt.Println(w)
		}
	}
	fmt.Println()

	nodes := make([]*NodeAgg, 0, len(byNode))
	for _, v := range byNode {
		nodes = append(nodes, v)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].SumDurationMs > nodes[j].SumDurationMs
	})

	fmt.Println("=== By node ===")
	fmt.Printf("%-35s  %10s  %10s  %14s  %16s  %16s\n", "NODE", "PODS", "CHUNKS", "SAMPLES", "SUM_CHUNK_MS", "MAX_CHUNK_MS")
	for _, n := range nodes {
		fmt.Printf("%-35s  %10d  %10d  %14d  %16d  %16d\n",
			n.Node, len(n.UniquePods), n.Chunks, n.Samples, n.SumDurationMs, n.MaxDurationMs)
	}
}
