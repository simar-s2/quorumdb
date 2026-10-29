#!/usr/bin/env bash
# Runs the benchmark or chaos Job inside the GKE cluster, shows its progress, and saves the JSON it
# prints at the end of its log to results/gke/<tool>.json.
#
#   scripts/gke-run-job.sh benchmark [extra benchmark flags, e.g. -repeats=1]
#   scripts/gke-run-job.sh chaos [extra chaos flags, e.g. -runs=5]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/gke-env.sh

tool=$1
shift
job=quorumdb-$tool
overlay=.deploy/$tool
if [ ! -f .deploy/image-tag ]; then
  echo "no image pushed yet: run make deploy first" >&2
  exit 1
fi
tag=$(cat .deploy/image-tag)
# The lines around the JSON in the log (defined in internal/results).
begin_marker="=== BEGIN RESULTS JSON ==="
end_marker="=== END RESULTS JSON ==="

# An overlay that points the Job at the pushed tools image and appends any extra flags to its
# arguments (a JSON patch "add" to the end of the args list).
mkdir -p "$overlay" results/gke
{
  echo "resources:"
  echo "- ../../deploy/k8s/$tool"
  echo "images:"
  echo "- name: quorumdb-tools"
  echo "  newName: $REGISTRY/tools"
  echo "  newTag: $tag"
  if [ $# -gt 0 ]; then
    echo "patches:"
    echo "- target: {kind: Job, name: $job}"
    echo "  patch: |-"
    for flag in "$@"; do
      echo "    - {op: add, path: /spec/template/spec/containers/0/args/-, value: \"$flag\"}"
    done
  fi
} > "$overlay/kustomization.yaml"

# A Job cannot be changed once created, so delete the previous run's Job (and its log) first.
kube -n "$NAMESPACE" delete job "$job" --ignore-not-found --wait=true
kube apply -k "$overlay"

echo "waiting for the $job pod to start..."
until kube -n "$NAMESPACE" logs "job/$job" >/dev/null 2>&1; do sleep 2; done

# Show progress as it happens, without the JSON. If the stream drops, the loop below keeps waiting.
kube -n "$NAMESPACE" logs -f "job/$job" | sed "/^$begin_marker\$/,/^$end_marker\$/d" || true

log=$overlay/job.log
while true; do
  kube -n "$NAMESPACE" logs "job/$job" > "$log" 2>/dev/null || true
  if grep -q "^$end_marker\$" "$log"; then
    break
  fi
  if [ -z "$(kube -n "$NAMESPACE" get job "$job" -o jsonpath='{.status.active}')" ]; then
    echo "the Job finished without printing results; its log is in $log" >&2
    exit 1
  fi
  sleep 10
done
sed -n "/^$begin_marker\$/,/^$end_marker\$/p" "$log" | sed '1d;$d' > "results/gke/$tool.json"
echo "saved results/gke/$tool.json (full log in $log)"
