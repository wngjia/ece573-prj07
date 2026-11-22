#!/usr/bin/env bash
set -euo pipefail

for tool in helm kubectl kind; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }
done
cd "$(dirname "$0")/.."
kind export kubeconfig --name ece573-prj07 >/dev/null
helm repo add chaos-mesh https://charts.chaos-mesh.org --force-update >/dev/null
helm repo update chaos-mesh >/dev/null
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --version 2.6.2 --namespace chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock
kubectl rollout status deployment/chaos-controller-manager -n chaos-mesh --timeout=5m
kubectl rollout status daemonset/chaos-daemon -n chaos-mesh --timeout=5m
