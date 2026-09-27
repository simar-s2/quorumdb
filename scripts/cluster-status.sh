#!/usr/bin/env bash
# Prints each node's Raft role, term and log position.
set -euo pipefail
cd "$(dirname "$0")/.."
printf "%-5s %-10s %-6s %-7s %-13s %-12s %s\n" NODE ROLE TERM LEADER COMMIT_INDEX LAST_APPLIED KEYS
for n in n1 n2 n3; do
  if ! info=$(docker compose exec -T "$n" redis-cli INFO 2>/dev/null | tr -d '\r'); then
    printf "%-5s %s\n" "$n" "down"
    continue
  fi
  field() { echo "$info" | awk -F: -v k="$1" '$1==k {print $2}'; }
  keys=$(field db0 | sed 's/keys=//')
  printf "%-5s %-10s %-6s %-7s %-13s %-12s %s\n" "$n" "$(field role)" "$(field term)" "$(field leader_id)" \
    "$(field commit_index)" "$(field last_applied)" "$keys"
done
