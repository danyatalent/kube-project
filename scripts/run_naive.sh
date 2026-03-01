#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-IMAGE_HERE}"
K="${K:-30}"
TOTAL_SAMPLES="${TOTAL_SAMPLES:-1500000000}"

TASK_TYPE="${TASK_TYPE:-pi}" # pi | integral
INTEGRAL_A="${INTEGRAL_A:-0}"
INTEGRAL_B="${INTEGRAL_B:-3.141592653589793}"
INTEGRAL_EXPR="${INTEGRAL_EXPR:-sin(x)}"

SAMPLES_PER_POD=$(( TOTAL_SAMPLES / K ))

TMP="$(mktemp)"
cp k8s/job-naive-parallel-indexed.yaml "$TMP"

sed -i "s|IMAGE_HERE|$IMAGE|g" "$TMP"
sed -i "s/completions: 30/completions: $K/g" "$TMP"
sed -i "s/parallelism: 30/parallelism: $K/g" "$TMP"
sed -i "s/value: \"50000000\"/value: \"$SAMPLES_PER_POD\"/g" "$TMP"

# Установим TASK_TYPE и integral params
# (важно: expr кладём в yaml как обычную строку; избегай кавычек внутри expr)
# Если нужен пробел — можно: "sin(x) + x*x" (в YAML это ок)
perl -0777 -i -pe "s/name: TASK_TYPE\\n\\s*value: \"pi\"/name: TASK_TYPE\\n              value: \"$TASK_TYPE\"/g" "$TMP"
perl -0777 -i -pe "s/name: INTEGRAL_A\\n\\s*value: \".*?\"/name: INTEGRAL_A\\n              value: \"$INTEGRAL_A\"/g" "$TMP"
perl -0777 -i -pe "s/name: INTEGRAL_B\\n\\s*value: \".*?\"/name: INTEGRAL_B\\n              value: \"$INTEGRAL_B\"/g" "$TMP"
perl -0777 -i -pe "s/name: INTEGRAL_EXPR\\n\\s*value: \".*?\"/name: INTEGRAL_EXPR\\n              value: \"$INTEGRAL_EXPR\"/g" "$TMP"

kubectl delete job mc-naive-parallel --ignore-not-found

echo "Running naive parallel:"
echo "  TASK_TYPE=$TASK_TYPE"
echo "  K=$K TOTAL_SAMPLES=$TOTAL_SAMPLES PER_POD=$SAMPLES_PER_POD"
if [[ "$TASK_TYPE" == "integral" ]]; then
  echo "  integral: a=$INTEGRAL_A b=$INTEGRAL_B expr=$INTEGRAL_EXPR"
fi

START_NS=$(date +%s%N)
kubectl apply -f "$TMP"
kubectl wait --for=condition=complete job/mc-naive-parallel --timeout=3600s
END_NS=$(date +%s%N)

WALL_MS=$(( (END_NS - START_NS) / 1000000 ))
echo "WALL_CLOCK_MS=$WALL_MS"

kubectl get pods -l job-name=mc-naive-parallel -o wide

rm -f "$TMP"