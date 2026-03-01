#!/usr/bin/env bash
set -euo pipefail

JOB="${1:?usage: $0 <job-name>}"

echo "Collecting logs from job=$JOB ..."
PODS=($(kubectl get pods -l job-name="$JOB" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'))

: > /tmp/mc_logs.jsonl
for p in "${PODS[@]}"; do
  kubectl logs "$p" >> /tmp/mc_logs.jsonl
done

echo "---- summary ----"
./mc-summary < /tmp/mc_logs.jsonl