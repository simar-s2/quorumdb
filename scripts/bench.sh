#!/usr/bin/env bash
# Runs redis-benchmark from a container on the cluster network against the leader (or TARGET=follower).
set -euo pipefail
cd "$(dirname "$0")/.."

REQUESTS=${REQUESTS:-200000}
CLIENTS=${CLIENTS:-50}
PIPELINE=${PIPELINE:-1}
KEYSPACE=${KEYSPACE:-100000}
TESTS=${TESTS:-set,get}
TARGET=${TARGET:-leader}

leader=""
follower=""
for n in n1 n2 n3; do
  role=$(docker compose exec -T "$n" redis-cli INFO raft 2>/dev/null | tr -d '\r' | awk -F: '$1=="role" {print $2}') || true
  case "$role" in
    leader) leader=$n ;;
    follower) [ -z "$follower" ] && follower=$n ;;
  esac
done
if [ -z "$leader" ]; then
  echo "no leader; is the cluster up? (make cluster-up)" >&2
  exit 1
fi
host=$leader
[ "$TARGET" = follower ] && host=$follower

args=(-h "$host" -p 6379 -t "$TESTS" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -r "$KEYSPACE" --csv)
echo "target: $host ($TARGET)   leader: $leader"
echo "redis-benchmark ${args[*]}"
csv=$(docker compose run --rm bench "${args[@]}" 2>/dev/null | tr -d '\r')
if [ -n "${RAW:-}" ]; then
  echo "$csv"
  exit 0
fi
echo
echo "$csv" | awk -F'","' 'NR == 1 {printf "%-6s %12s %9s %9s %9s %9s %9s\n", "test", "requests/s", "avg ms", "p50 ms", "p95 ms", "p99 ms", "max ms"; next}
  {gsub(/"/, ""); printf "%-6s %12s %9s %9s %9s %9s %9s\n", $1, $2, $3, $5, $6, $7, $8}'
