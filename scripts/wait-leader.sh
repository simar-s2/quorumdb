#!/usr/bin/env bash
# Waits until the compose cluster has elected a leader, then prints status.
set -euo pipefail
cd "$(dirname "$0")/.."
for _ in $(seq 1 100); do
  for n in n1 n2 n3; do
    if docker compose exec -T "$n" redis-cli INFO raft 2>/dev/null | tr -d '\r' | grep -q '^role:leader'; then
      scripts/cluster-status.sh
      exit 0
    fi
  done
  sleep 0.2
done
echo "no leader elected after 20s" >&2
exit 1
