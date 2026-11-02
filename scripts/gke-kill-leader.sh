#!/usr/bin/env bash
# Crashes the leader pod on GKE (kill -9, then deletes the pod so Kubernetes starts a new one), to
# watch the nodes in the other zones elect a new leader. Same crash as the chaos test.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/gke-env.sh

for pod in quorumdb-0 quorumdb-1 quorumdb-2; do
  if kube -n "$NAMESPACE" exec "$pod" -- redis-cli INFO raft 2>/dev/null | tr -d '\r' | grep -q '^role:leader'; then
    node=$(kube -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}')
    zone=$(kube get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
    # The exec fails because the container dies with the server; that is expected.
    kube -n "$NAMESPACE" exec "$pod" -- sh -c 'kill -KILL $(pidof quorumdb)' >/dev/null 2>&1 || true
    kube -n "$NAMESPACE" delete pod "$pod" --wait=false >/dev/null
    echo "crashed $pod, the leader, in $zone; Kubernetes is starting a new pod for it"
    exit 0
  fi
done
echo "no leader found" >&2
exit 1
