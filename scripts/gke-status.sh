#!/usr/bin/env bash
# Prints each QuorumDB pod's zone, node, Raft role, term and log position.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/gke-env.sh

printf "%-11s %-12s %-10s %-5s %-11s %-13s %s\n" POD ZONE ROLE TERM LEADER COMMIT_INDEX LAST_APPLIED
for pod in quorumdb-0 quorumdb-1 quorumdb-2; do
  node=$(kube -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
  zone=$( [ -n "$node" ] && kube get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}' || echo "-")
  if ! info=$(kube -n "$NAMESPACE" exec "$pod" -- redis-cli INFO raft 2>/dev/null | tr -d '\r'); then
    printf "%-11s %-12s %s\n" "$pod" "$zone" "down"
    continue
  fi
  field() { echo "$info" | awk -F: -v key="$1" '$1 == key {print $2}'; }
  printf "%-11s %-12s %-10s %-5s %-11s %-13s %s\n" "$pod" "$zone" "$(field role)" "$(field term)" \
    "$(field leader_id)" "$(field commit_index)" "$(field last_applied)"
done
