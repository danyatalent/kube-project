#!/usr/bin/env bash
set -euo pipefail

# Переменные (можно переопределить через export)
IMAGE="${IMAGE:-danekw8/mc-worker:0.3}"
TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"

K_MIN="${K_MIN:-20}"
K_MAX="${K_MAX:-240}"
K_STEP="${K_STEP:-20}"

TEMPLATE="${TEMPLATE:-./job-naive.yaml}"          # путь к шаблону
JOB_NAME="${JOB_NAME:-mc-naive-sweep}"

OUT_CSV="${OUT_CSV:-./mc_k_sweep_full.csv}"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template not found: $TEMPLATE" >&2
  exit 2
fi
if [[ ! -x ./mc-summary ]]; then
  echo "mc-summary not found in current directory or not executable" >&2
  exit 2
fi

# === 1. Получаем список всех узлов кластера ===
mapfile -t NODES < <(kubectl get nodes -o name | sed 's|node/||')
if [[ ${#NODES[@]} -eq 0 ]]; then
    echo "No nodes found in the cluster!" >&2
    exit 2
fi
echo "Detected nodes: ${NODES[*]}"

# === 2. Формируем заголовок CSV ===
HEADER="k,total_samples,samples_per_pod,wall_ms,avg_pod_ms,max_pod_ms,pods"
for node in "${NODES[@]}"; do
    HEADER="$HEADER,$node"
done
echo "$HEADER" > "$OUT_CSV"

# === 3. Основной цикл по K ===
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

  # Подстановка параметров в YAML
  sed -i "s|__K__|$K|g" "$TMP"
  sed -i "s|__SAMPLES__|$SAMPLES_PER_POD|g" "$TMP"

  RUN_JOB="${JOB_NAME}-k${K}"
  sed -i "s/name: mc-naive-sweep/name: ${RUN_JOB}/g" "$TMP"

  # Сухая проверка
  kubectl apply --dry-run=client -f "$TMP" >/dev/null

  # Удаляем старый job, если есть
  kubectl delete job "${RUN_JOB}" --ignore-not-found >/dev/null 2>&1 || true

  # Запускаем job и ждём завершения
  START_NS=$(date +%s%N)
  kubectl apply -f "$TMP" >/dev/null
  kubectl wait --for=condition=complete "job/${RUN_JOB}" --timeout=3600s >/dev/null
  END_NS=$(date +%s%N)

  WALL_MS=$(( (END_NS - START_NS) / 1000000 ))

  # Собираем логи и запускаем mc-summary для получения статистики
  LOG_FILE="/tmp/${RUN_JOB}.jsonl"
  kubectl logs -l job-name="${RUN_JOB}" > "$LOG_FILE"

  SUM_TXT="/tmp/${RUN_JOB}_summary.txt"
  ./mc-summary < "$LOG_FILE" | tee "$SUM_TXT" >/dev/null

  # Парсим общую информацию из mc-summary
  PODS=$(awk '/^Pods:/ {print $2}' "$SUM_TXT")
  AVG=$(awk '/^Avg pod duration:/ {print $4}' "$SUM_TXT")
  MAX=$(awk '/^Max pod duration/ {print $5}' "$SUM_TXT")

  # === 4. Получаем распределение подов по узлам ===
  # Инициализируем ассоциативный массив нулями для всех узлов
  declare -A node_count
  for node in "${NODES[@]}"; do
      node_count["$node"]=0
  done

  # Собираем имена узлов для успешно завершившихся подов
  while read -r node; do
      if [[ -n "$node" ]] && [[ ${node_count[$node]+_} ]]; then
          ((node_count["$node"]++))
      fi
  done < <(kubectl get pods -l job-name="${RUN_JOB}" --field-selector status.phase=Succeeded -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}')

  # Формируем строку данных CSV
  DATA_LINE="$K,$TOTAL_SAMPLES,$SAMPLES_PER_POD,$WALL_MS,$AVG,$MAX,$PODS"
  for node in "${NODES[@]}"; do
      DATA_LINE="$DATA_LINE,${node_count[$node]}"
  done
  echo "$DATA_LINE" >> "$OUT_CSV"

  # Очистка временных файлов и job'а
  rm -f "$TMP"
  kubectl delete job "${RUN_JOB}" --ignore-not-found >/dev/null 2>&1 || true
done

echo
echo "DONE. CSV saved to: $OUT_CSV"