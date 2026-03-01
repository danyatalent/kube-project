#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-fixed}" # fixed|adaptive

TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"
BASE_SEED="${BASE_SEED:-1}"

# fixed:
CHUNK_SAMPLES="${CHUNK_SAMPLES:-15000000}"

# adaptive:
MIN_CHUNK="${MIN_CHUNK:-5000000}"
MAX_CHUNK="${MAX_CHUNK:-30000000}"
BETA="${BETA:-0.10}"
ALPHA="${ALPHA:-0.30}"

# task params:
TASK_TYPE="${TASK_TYPE:-integral}"
INTEGRAL_DIM="${INTEGRAL_DIM:-8}"
INTEGRAL_A="${INTEGRAL_A:-0}"
INTEGRAL_B="${INTEGRAL_B:-15}"
INTEGRAL_EXPR="${INTEGRAL_EXPR:-exp(-x1)*sin(20*x1) + 0.25*cos(x5) - 2*pow(x6,3) + 0.4*tan(x7) + 1.5*cos(pow(x8,5)) + 0.1*pow(x2,4)*cos(7*x3*x3) + sin(x4)}"

if [[ ! -x ./mc-summary ]]; then
  echo "ERROR: ./mc-summary not found or not executable" >&2
  exit 2
fi

echo "Port-forward dispatcher..."
kubectl port-forward deploy/mc-dispatcher 8080:8080 >/tmp/mc_pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID >/dev/null 2>&1 || true' EXIT
sleep 1

if [[ "$MODE" == "fixed" ]]; then
  PAYLOAD=$(cat <<JSON
{
  "mode":"fixed",
  "task_type":"$TASK_TYPE",
  "total_samples":$TOTAL_SAMPLES,
  "base_seed":$BASE_SEED,
  "chunk_samples":$CHUNK_SAMPLES,
  "dim":$INTEGRAL_DIM,
  "a":$INTEGRAL_A,
  "b":$INTEGRAL_B,
  "expr":"$INTEGRAL_EXPR"
}
JSON
)
else
  PAYLOAD=$(cat <<JSON
{
  "mode":"adaptive",
  "task_type":"$TASK_TYPE",
  "total_samples":$TOTAL_SAMPLES,
  "base_seed":$BASE_SEED,
  "min_chunk":$MIN_CHUNK,
  "max_chunk":$MAX_CHUNK,
  "beta":$BETA,
  "alpha":$ALPHA,
  "dim":$INTEGRAL_DIM,
  "a":$INTEGRAL_A,
  "b":$INTEGRAL_B,
  "expr":"$INTEGRAL_EXPR"
}
JSON
)
fi

echo "Starting run: mode=$MODE total=$TOTAL_SAMPLES"
curl -s -X POST http://127.0.0.1:8080/run -H 'Content-Type: application/json' -d "$PAYLOAD"
echo

echo "Waiting..."
while true; do
  ST=$(curl -s http://127.0.0.1:8080/status)
  echo "$ST"
  RUNNING=$(echo "$ST" | sed -n 's/.*"running":\([a-z]*\).*/\1/p')
  if [[ "$RUNNING" == "false" ]]; then
    WALL=$(echo "$ST" | sed -n 's/.*"wall_ms":\([0-9]*\).*/\1/p')
    break
  fi
  sleep 2
done

OUT="/tmp/mc-pull-${MODE}-$(date +%s).jsonl"
curl -s http://127.0.0.1:8080/results > "$OUT"

echo "---- summary ----"
./mc-summary < "$OUT"
echo
echo "WALL_MS=$WALL"
echo "JSONL=$OUT"