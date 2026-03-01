#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-danekw8/mc-worker:0.3}"
TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"

# список K можно переопределить: K_LIST="20 40 80 160"
K_LIST="${K_LIST:-20 40 80 120 160 240}"

TEMPLATE="${TEMPLATE:-k8s/job-naive.yaml}"
JOB_NAME="${JOB_NAME:-mc-naive-sweep}"

OUT_CSV="${OUT_CSV:-/tmp/mc_k_sweep.csv}"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template not found: $TEMPLATE" >&2
  exit 2
fi

if [[ ! -x "./mc-summary" ]]; then
  echo "mc-summary not found in project root. Put it here or run from root." >&2
  exit 2
fi

echo "k_sweep: IMAGE=$IMAGE TOTAL_SAMPLES=$TOTAL_SAMPLES"
echo "K_LIST=$K_LIST"
echo "Writing results to $OUT_CSV"

echo "k,total_samples,samples_per_pod,wall_ms,avg_pod_ms,max_pod_ms,pods,node_kubeadm1,node_kubeadm2,node_kubeadm3strong" > "$OUT_CSV"

for K in $K_LIST; do
  if (( K <= 0 )); then
    echo "Skipping invalid K=$K"
    continue
  fi

  SAMPLES_PER_POD=$(( TOTAL_SAMPLES / K ))
  if (( SAMPLES_PER_POD <= 0 )); then
    echo "Skipping K=$K because samples_per_pod=0"
    continue
  fi

  echo
  echo "=== Run K=$K (samples_per_pod=$SAMPLES_PER_POD) ==="

  TMP="$(mktemp)"
  cp "$TEMPLATE" "$TMP"

  # Подставляем image / k / samples
  sed -i "s|IMAGE_HERE|$IMAGE|g" "$TMP"
  sed -i "s|__K__|$K|g" "$TMP"
  sed -i "s|__SAMPLES__|$SAMPLES_PER_POD|g" "$TMP"

  # Переименуем job внутри YAML, чтобы не конфликтовал
  # Меняем только metadata.name:
  sed -i "s/name: mc-naive-parallel-second/name: $JOB_NAME/g" "$TMP"

  # Проверка YAML
  kubectl apply --dry-run=client -f "$TMP" >/dev/null

  kubectl delete job "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true

  START_NS=$(date +%s%N)
  kubectl apply -f "$TMP" >/dev/null
  kubectl wait --for=condition=complete "job/$JOB_NAME" --timeout=3600s >/dev/null
  END_NS=$(date +%s%N)
  WALL_MS=$(( (END_NS - START_NS) / 1000000 ))

  LOG_FILE="/tmp/${JOB_NAME}_K${K}.jsonl"
  kubectl logs -l job-name="$JOB_NAME" > "$LOG_FILE"

  # summary в текст + парсинг ключевых чисел
  SUM_TXT="/tmp/${JOB_NAME}_K${K}_summary.txt"
  ./mc-summary < "$LOG_FILE" | tee "$SUM_TXT" >/dev/null

  PODS=$(grep -E '^Pods:' "$SUM_TXT" | awk '{print $2}' || echo "")
  AVG_POD_MS=$(grep -E '^Avg pod duration:' "$SUM_TXT" | awk '{print $4}' || echo "")
  MAX_POD_MS=$(grep -E '^Max pod duration' "$SUM_TXT" | awk '{print $5}' || echo "")

  # по нодам (если их нет — будет 0)
  N1=$(awk '/^kubeadm1[[:space:]]/ {print $2}' "$SUM_TXT" 2>/dev/null || true)
  N2=$(awk '/^kubeadm2[[:space:]]/ {print $2}' "$SUM_TXT" 2>/dev/null || true)
  N3=$(awk '/^kubeadm3-strong[[:space:]]/ {print $2}' "$SUM_TXT" 2>/dev/null || true)
  N1=${N1:-0}; N2=${N2:-0}; N3=${N3:-0}

  echo "K=$K WALL_MS=$WALL_MS AVG_POD_MS=$AVG_POD_MS MAX_POD_MS=$MAX_POD_MS LOG=$LOG_FILE"

  echo "$K,$TOTAL_SAMPLES,$SAMPLES_PER_POD,$WALL_MS,$AVG_POD_MS,$MAX_POD_MS,$PODS,$N1,$N2,$N3" >> "$OUT_CSV"

  rm -f "$TMP"

  # чистим job, чтобы не копить поды (можешь закомментировать если хочешь оставлять)
  kubectl delete job "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
done

echo
echo "Done. CSV: $OUT_CSV"