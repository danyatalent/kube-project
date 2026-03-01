package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/Knetic/govaluate"
)

type Result struct {
	TaskType string `json:"task_type"` // "pi" | "integral"

	// integral params
	A    float64 `json:"a,omitempty"`
	B    float64 `json:"b,omitempty"`
	Dim  int     `json:"dim,omitempty"`
	Func string  `json:"func,omitempty"` // legacy named function (1D)
	Expr string  `json:"expr,omitempty"` // expression (supports x1..xd)

	TaskID   int     `json:"task_id"`
	Samples  int64   `json:"samples"`
	Acc      float64 `json:"acc"` // hits (pi) or sum f(x) (integral)
	Estimate float64 `json:"estimate"`

	DurationMs int64  `json:"duration_ms"`
	Node       string `json:"node"`
	Pod        string `json:"pod"`
	GoMaxProcs int    `json:"gomaxprocs"`
	Seed       int64  `json:"seed"`
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

// legacy named functions for 1D (можно оставить)
func fNamed(name string, x float64) float64 {
	switch name {
	case "sin":
		return math.Sin(x)
	case "poly2":
		return x * x
	case "expnegx2":
		return math.Exp(-x * x)
	default:
		return x
	}
}

func toFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	default:
		return math.NaN()
	}
}

func compileExpr(expr string) (*govaluate.EvaluableExpression, error) {
	functions := map[string]govaluate.ExpressionFunction{
		"sin":  func(args ...interface{}) (interface{}, error) { return math.Sin(toFloat(args[0])), nil },
		"cos":  func(args ...interface{}) (interface{}, error) { return math.Cos(toFloat(args[0])), nil },
		"tan":  func(args ...interface{}) (interface{}, error) { return math.Tan(toFloat(args[0])), nil },
		"exp":  func(args ...interface{}) (interface{}, error) { return math.Exp(toFloat(args[0])), nil },
		"log":  func(args ...interface{}) (interface{}, error) { return math.Log(toFloat(args[0])), nil },
		"sqrt": func(args ...interface{}) (interface{}, error) { return math.Sqrt(toFloat(args[0])), nil },
		"abs":  func(args ...interface{}) (interface{}, error) { return math.Abs(toFloat(args[0])), nil },
		"pow": func(args ...interface{}) (interface{}, error) {
			return math.Pow(toFloat(args[0]), toFloat(args[1])), nil
		},
		"min": func(args ...interface{}) (interface{}, error) {
			a := toFloat(args[0])
			b := toFloat(args[1])
			if a < b {
				return a, nil
			}
			return b, nil
		},
		"max": func(args ...interface{}) (interface{}, error) {
			a := toFloat(args[0])
			b := toFloat(args[1])
			if a > b {
				return a, nil
			}
			return b, nil
		},
	}

	return govaluate.NewEvaluableExpressionWithFunctions(expr, functions)
}

func runPi(r *rand.Rand, samples int64) (hits int64, estimate float64) {
	for i := int64(0); i < samples; i++ {
		x := r.Float64()
		y := r.Float64()
		if x*x+y*y <= 1.0 {
			hits++
		}
	}
	estimate = 4.0 * float64(hits) / float64(samples)
	return hits, estimate
}

// 1D integral with named function
func runIntegralNamed1D(r *rand.Rand, samples int64, a, b float64, fn string) (sum float64, estimate float64) {
	width := b - a
	for i := int64(0); i < samples; i++ {
		x := a + r.Float64()*width
		sum += fNamed(fn, x)
	}
	estimate = width * (sum / float64(samples))
	return sum, estimate
}

// d-dimensional integral with expression (variables x1..xd)
func runIntegralExprND(r *rand.Rand, samples int64, a, b float64, dim int, expr *govaluate.EvaluableExpression) (sum float64, estimate float64, evalErrors int64) {
	width := b - a
	volume := math.Pow(width, float64(dim))

	// заранее подготовим параметры и ключи x1..xd
	params := make(map[string]interface{}, dim)
	keys := make([]string, dim)
	for i := 0; i < dim; i++ {
		keys[i] = fmt.Sprintf("x%d", i+1)
		params[keys[i]] = 0.0
	}

	for i := int64(0); i < samples; i++ {
		// генерируем точку в [a,b]^dim
		for j := 0; j < dim; j++ {
			x := a + r.Float64()*width
			params[keys[j]] = x
		}

		v, err := expr.Evaluate(params)
		if err != nil {
			evalErrors++
			continue
		}
		fx, ok := v.(float64)
		if !ok || math.IsNaN(fx) || math.IsInf(fx, 0) {
			evalErrors++
			continue
		}
		sum += fx
	}

	estimate = volume * (sum / float64(samples))
	return sum, estimate, evalErrors
}

func main() {
	var (
		taskType = flag.String("type", "pi", "task type: pi | integral")

		// integral
		fn   = flag.String("func", "sin", "named integrand (1D, used if expr is empty): sin | poly2 | expnegx2")
		expr = flag.String("expr", "", "integrand expression using x1..xd, e.g. \"sin(x1)+x2*x2\"")
		a    = flag.Float64("a", 0, "lower bound")
		b    = flag.Float64("b", math.Pi, "upper bound")
		dim  = flag.Int("dim", 1, "dimension for integral over [a,b]^dim (expr supports x1..xd)")

		samples = flag.Int64("samples", 50_000_000, "number of samples")
		seed    = flag.Int64("seed", 1, "random seed")
		taskID  = flag.Int("task", 0, "task id")
	)
	flag.Parse()

	start := time.Now()
	r := rand.New(rand.NewSource(*seed))

	var acc, estimate float64

	switch *taskType {
	case "pi":
		hits, est := runPi(r, *samples)
		acc = float64(hits)
		estimate = est

	case "integral":
		if *b <= *a {
			fmt.Fprintln(os.Stderr, "--b must be > --a")
			os.Exit(2)
		}
		if *dim < 1 || *dim > 32 {
			fmt.Fprintln(os.Stderr, "--dim must be in [1..32]")
			os.Exit(2)
		}

		if *expr != "" {
			compiled, err := compileExpr(*expr)
			if err != nil {
				fmt.Fprintln(os.Stderr, "bad --expr:", err)
				os.Exit(2)
			}
			sum, est, _ := runIntegralExprND(r, *samples, *a, *b, *dim, compiled)
			acc = sum
			estimate = est
		} else {
			// legacy: 1D named
			if *dim != 1 {
				fmt.Fprintln(os.Stderr, "named --func supports only --dim=1; use --expr for dim>1")
				os.Exit(2)
			}
			sum, est := runIntegralNamed1D(r, *samples, *a, *b, *fn)
			acc = sum
			estimate = est
		}

	default:
		fmt.Fprintln(os.Stderr, "unknown --type, use pi or integral")
		os.Exit(2)
	}

	res := Result{
		TaskType:   *taskType,
		A:          *a,
		B:          *b,
		Dim:        *dim,
		Func:       *fn,
		Expr:       *expr,
		TaskID:     *taskID,
		Samples:    *samples,
		Acc:        acc,
		Estimate:   estimate,
		DurationMs: time.Since(start).Milliseconds(),
		Node:       getenv("NODE_NAME", "unknown"),
		Pod:        getenv("POD_NAME", "unknown"),
		GoMaxProcs: runtime.GOMAXPROCS(0),
		Seed:       *seed,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(res)
}
