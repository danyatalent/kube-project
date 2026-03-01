#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-danekw8/mc-worker:0.3}"
TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"

# шаг можно менять: 10, 20 и т.д.
K_MIN="${K_MIN:-20}"
K_MAX="${K_MAX:-240}"
K_STEP="${K_STEP:-20}"

TEMPLATE="${TEMPLATE:-k8s/job-naive.yaml}"
JOB_NAME="${JOB_NAME:-mc-naive-sweep}"

OUT_CSV="${OUT_CSV:-/tmp/mc_k_sweep_full.csv}"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template not found: $TEMPLATE" >&2
  exit 2
fi
if [[ ! -x ./mc-summary ]]; then
  echo "mc-summary not found in current directory or not executable" >&2
  exit 2
fi

echo "k,total_samples,samples_per_pod,wall_ms,avg_pod_ms,max_pod_ms,pods,node_kubeadm1,node_kubeadm2,node_kubeadm3strong" > "$OUT_CSV"

for ((K=K_MIN; K<=K_MAX; K+=K_STEP)); do
  SAMPLES_PER_POD=$(( TOTAL_SAMPLES / K ))
  if (( SAMPLES_PER_POD <= 0 )); then
    echo "Skipping K=$K because samples_per_pod=0"
    continue
  fi

  echo
  echo "=== K=$K, samples_per_pod=$SAMPLES_PER_POD ==="

  TMP="$(mktemp)"
  cp "$TEMPLATE" "$TMP"

  # подстановка
  sed -i "s|IMAGE_HERE|$IMAGE|g" "$TMP"
  sed -i "s|__K__|$K|g" "$TMP"
  sed -i "s|__SAMPLES__|$SAMPLES_PER_POD|g" "$TMP"

  # Уникальное имя job, чтобы не путаться в логах
  RUN_JOB="${JOB_NAME}-k${K}"
  sed -i "s/name: mc-naive-sweep/name: ${RUN_JOB}/g" "$TMP"

  # проверка YAML
  kubectl apply --dry-run=client -f "$TMP" >/dev/null

  # чистим прошлый запуск (если был)
  kubectl delete job "${RUN_JOB}" --ignore-not-found >/dev/null 2>&1 || true

  START_NS=$(date +%s%N)
  kubectl apply -f "$TMP" >/dev/null
  kubectl wait --for=condition=complete "job/${RUN_JOB}" --timeout=3600s >/dev/null
  END_NS=$(date +%s%N)

  WALL_MS=$(( (END_NS - START_NS) / 1000000 ))

  LOG_FILE="/tmp/${RUN_JOB}.jsonl"
  kubectl logs -l job-name="${RUN_JOB}" > "$LOG_FILE"

  SUM_TXT="/tmp/${RUN_JOB}_summary.txt"
  ./mc-summary < "$LOG_FILE" | tee "$SUM_TXT" >/dev/null

  PODS=$(awk '/^Pods:/ {print $2}' "$SUM_TXT")
  AVG=$(awk '/^Avg pod duration:/ {print $4}' "$SUM_TXT")
  MAX=$(awk '/^Max pod duration/ {print $5}' "$SUM_TXT")

  N1=$(awk '/^kubeadm1[[:space:]]/ {print $2}' "$SUM_TXT" || true); N1=${N1:-0}
  N2=$(awk '/^kubeadm2[[:space:]]/ {print $2}' "$SUM_TXT" || true); N2=${N2:-0}
  N3=$(awk '/^kubeadm3-strong[[:space:]]/ {print $2}' "$SUM_TXT" || true); N3=${N3:-0}

  echo "K=$K WALL_MS=$WALL_MS AVG_POD_MS=$AVG MAX_POD_MS=$MAX"
  echo "$K,$TOTAL_SAMPLES,$SAMPLES_PER_POD,$WALL_MS,$AVG,$MAX,$PODS,$N1,$N2,$N3" >> "$OUT_CSV"

  rm -f "$TMP"

  # чтобы не копить историю в кластере
  kubectl delete job "${RUN_JOB}" --ignore-not-found >/dev/null 2>&1 || true
done

echo
echo "DONE. CSV saved to: $OUT_CSV"