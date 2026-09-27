#!/usr/bin/env bash
# Prints the name of the current leader node (n1, n2 or n3).
set -euo pipefail
cd "$(dirname "$0")/.."
for n in n1 n2 n3; do
  if docker compose exec -T "$n" redis-cli INFO raft 2>/dev/null | tr -d '\r' | grep -q '^role:leader'; then
    echo "$n"
    exit 0
  fi
done
echo "no leader" >&2
exit 1
