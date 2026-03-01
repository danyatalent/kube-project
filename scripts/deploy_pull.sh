#!/usr/bin/env bash
set -euo pipefail

DISPATCHER_IMAGE="${DISPATCHER_IMAGE:?set DISPATCHER_IMAGE}"
WORKER_IMAGE="${WORKER_IMAGE:?set WORKER_IMAGE}"

sed -i "s|IMAGE_DISPATCHER|$DISPATCHER_IMAGE|g" k8s/dispatcher.yaml
sed -i "s|IMAGE_WORKER|$WORKER_IMAGE|g" k8s/workers.yaml

kubectl apply -f k8s/dispatcher.yaml
kubectl apply -f k8s/workers.yaml

kubectl rollout status deploy/mc-dispatcher
kubectl rollout status deploy/mc-workers-kubeadm1
kubectl rollout status deploy/mc-workers-kubeadm2
kubectl rollout status deploy/mc-workers-kubeadm3

kubectl get pods -l app=mc-dispatcher
kubectl get pods -l app=mc-worker -o wide