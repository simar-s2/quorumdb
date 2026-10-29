#!/usr/bin/env bash
# Applies the QuorumDB manifests with the image that push-image.sh pushed, then waits until all three
# pods are ready and prints the cluster status.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/gke-env.sh

variant=${1:-base}
if [ ! -f .deploy/image-tag ]; then
  echo "no image pushed yet: run scripts/push-image.sh (or make deploy)" >&2
  exit 1
fi
tag=$(cat .deploy/image-tag)

# Kustomize cannot read the registry or tag from the environment, so write a two-line overlay
# that sets them on top of the manifests in deploy/k8s.
mkdir -p .deploy/quorumdb
cat > .deploy/quorumdb/kustomization.yaml <<YAML
resources:
- ../../deploy/k8s/$variant
images:
- name: quorumdb
  newName: $REGISTRY/quorumdb
  newTag: $tag
YAML

kube apply -k .deploy/quorumdb
kube -n "$NAMESPACE" rollout status statefulset/quorumdb --timeout=10m
scripts/gke-status.sh
