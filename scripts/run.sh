#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-IMAGE_HERE}"
K="${K:-30}"
TOTAL_SAMPLES="${TOTAL_SAMPLES:-1500000000}"

# TASK_TYPE/INTEGRAL_* теперь берутся из ConfigMap mc-task
# Эти переменные тут оставлять можно чисто для печати в консоль (не обязательно):
TASK_TYPE="${TASK_TYPE:-unknown}"
INTEGRAL_A="${INTEGRAL_A:-unknown}"
INTEGRAL_B="${INTEGRAL_B:-unknown}"
INTEGRAL_DIM="${INTEGRAL_DIM:-unknown}"
INTEGRAL_EXPR="${INTEGRAL_EXPR:-unknown}"

if (( K <= 0 )); then
  echo "K must be > 0" >&2
  exit 2
fi

SAMPLES_PER_POD=$(( TOTAL_SAMPLES / K ))
if (( SAMPLES_PER_POD <= 0 )); then
  echo "TOTAL_SAMPLES must be >= K" >&2
  exit 2
fi

TMP="$(mktemp)"
cp k8s/job-first.yaml "$TMP"

# Подмена только безопасных чисел/строк
sed -i "s|IMAGE_HERE|$IMAGE|g" "$TMP"
sed -i "s/completions: 30/completions: $K/g" "$TMP"
sed -i "s/parallelism: 30/parallelism: $K/g" "$TMP"
sed -i "s/value: \"50000000\"/value: \"$SAMPLES_PER_POD\"/g" "$TMP"

kubectl delete job mc-naive-parallel-second --ignore-not-found

echo "Running naive parallel:"
echo "  IMAGE=$IMAGE"
echo "  K=$K TOTAL_SAMPLES=$TOTAL_SAMPLES PER_POD=$SAMPLES_PER_POD"
echo "  (task params are read from ConfigMap mc-task)"
echo "  TASK_TYPE=$TASK_TYPE DIM=$INTEGRAL_DIM A=$INTEGRAL_A B=$INTEGRAL_B"
# expr печатать можно, но он длинный — оставлю как есть
if [[ "$INTEGRAL_EXPR" != "unknown" ]]; then
  echo "  EXPR=$INTEGRAL_EXPR"
fi

START_NS=$(date +%s%N)
kubectl apply -f "$TMP"
kubectl wait --for=condition=complete job/mc-naive-parallel --timeout=3600s
END_NS=$(date +%s%N)

WALL_MS=$(( (END_NS - START_NS) / 1000000 ))
echo "WALL_CLOCK_MS=$WALL_MS"

kubectl get pods -l job-name=mc-naive-parallel -o wide

rm -f "$TMP"