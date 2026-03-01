#!/usr/bin/env bash
set -euo pipefail

TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"

# список чанков (под твою задачу)
CHUNKS=(${CHUNKS:-"30000000 24000000 20000000 17142857 15000000 13333333 12000000 10000000 7500000 6000000 5000000"})

OUT_CSV="${OUT_CSV:-/tmp/mc_pull_fixed_sweep.csv}"

echo "chunk_samples,total_samples,wall_ms,avg_pod_ms,max_pod_ms" > "$OUT_CSV"

for cs in "${CHUNKS[@]}"; do
  echo
  echo "=== fixed chunk=$cs ==="
  MODE=fixed TOTAL_SAMPLES="$TOTAL_SAMPLES" CHUNK_SAMPLES="$cs" bash scripts/run_pull_once.sh | tee /tmp/_last_run.txt

  WALL=$(grep -E '^WALL_MS=' /tmp/_last_run.txt | tail -n1 | cut -d= -f2)
  AVG=$(grep -E '^Avg pod duration:' -m1 /tmp/_last_run.txt | awk '{print $4}')
  MAX=$(grep -E '^Max pod duration' -m1 /tmp/_last_run.txt | awk '{print $5}')

  echo "$cs,$TOTAL_SAMPLES,$WALL,$AVG,$MAX" >> "$OUT_CSV"
done

echo "DONE: $OUT_CSV"