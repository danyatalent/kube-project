#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-IMAGE_HERE}"
TOTAL_SAMPLES="${TOTAL_SAMPLES:-1200000000}"

# В seq мы хотим тестить именно mc-worker-ноды (без control-plane taint)
# Самый простой и надёжный вариант — явный список:
NODES=(${NODES_OVERRIDE:-"kubeadm1 kubeadm3-strong"})

echo "Nodes: ${NODES[*]}"

for n in "${NODES[@]}"; do
  JOB="mc-seq-${n//./-}"

  TMP="$(mktemp)"
  cp k8s/job-seq-template.yaml "$TMP"

  sed -i "s|IMAGE_HERE|$IMAGE|g" "$TMP"
  sed -i "s|JOB_NAME_HERE|$JOB|g" "$TMP"
  sed -i "s|NODE_NAME_HERE|$n|g" "$TMP"
  sed -i "s/value: \"200000000\"/value: \"$TOTAL_SAMPLES\"/g" "$TMP"

  kubectl delete job "$JOB" --ignore-not-found

  echo "Running sequential on node=$n job=$JOB"
  echo "  IMAGE=$IMAGE TOTAL_SAMPLES=$TOTAL_SAMPLES (task params from ConfigMap mc-task)"

  START_NS=$(date +%s%N)
  kubectl apply -f "$TMP"
  kubectl wait --for=condition=complete "job/$JOB" --timeout=3600s
  END_NS=$(date +%s%N)

  WALL_MS=$(( (END_NS - START_NS) / 1000000 ))
  echo "node=$n WALL_CLOCK_MS=$WALL_MS"

  kubectl get pods -l job-name="$JOB" -o wide

  rm -f "$TMP"
done