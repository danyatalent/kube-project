#!/usr/bin/env bash
set -euo pipefail

TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"
OUT_CSV="${OUT_CSV:-/tmp/mc_pull_adaptive_sweep.csv}"

# Нормальные стартовые сетки
BETAS=(${BETAS:-"0.05 0.10 0.20"})
ALPHAS=(${ALPHAS:-"0.20 0.30 0.50"})
MIN_CHUNK="${MIN_CHUNK:-5000000}"
MAX_CHUNK="${MAX_CHUNK:-30000000}"

echo "beta,alpha,min_chunk,max_chunk,total_samples,wall_ms,avg_pod_ms,max_pod_ms" > "$OUT_CSV"

for b in "${BETAS[@]}"; do
  for a in "${ALPHAS[@]}"; do
    echo
    echo "=== adaptive beta=$b alpha=$a ==="
    MODE=adaptive TOTAL_SAMPLES="$TOTAL_SAMPLES" BETA="$b" ALPHA="$a" MIN_CHUNK="$MIN_CHUNK" MAX_CHUNK="$MAX_CHUNK" \
      bash scripts/run_pull_once.sh | tee /tmp/_last_run.txt

    WALL=$(grep -E '^WALL_MS=' /tmp/_last_run.txt | tail -n1 | cut -d= -f2)
    AVG=$(grep -E '^Avg pod duration:' -m1 /tmp/_last_run.txt | awk '{print $4}')
    MAX=$(grep -E '^Max pod duration' -m1 /tmp/_last_run.txt | awk '{print $5}')

    echo "$b,$a,$MIN_CHUNK,$MAX_CHUNK,$TOTAL_SAMPLES,$WALL,$AVG,$MAX" >> "$OUT_CSV"
  done
done

echo "DONE: $OUT_CSV"