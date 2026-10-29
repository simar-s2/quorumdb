#!/usr/bin/env bash
# First half of `make down`: deletes QuorumDB and its disks while the cluster still exists.
#
# The persistent disks were created by the cluster for the PersistentVolumeClaims, not by Terraform,
# so `terraform destroy` does not know about them. Deleting the cluster first would leave them
# behind, still billed. Deleting the namespace deletes the claims, and the cluster then deletes
# the disks.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/gke-env.sh

if ! kube get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "namespace $NAMESPACE not found in $KUBE_CONTEXT (already deleted, or no cluster); skipping"
  exit 0
fi
kube delete namespace "$NAMESPACE" --wait=true --timeout=10m

echo "waiting for the persistent disks to be deleted..."
for _ in $(seq 1 60); do
  remaining=$(kube get pv -o jsonpath="{range .items[?(@.spec.claimRef.namespace==\"$NAMESPACE\")]}{.metadata.name}{\"\\n\"}{end}")
  if [ -z "$remaining" ]; then
    echo "all QuorumDB disks deleted"
    exit 0
  fi
  sleep 5
done
echo "some PersistentVolumes still exist; check with: kubectl get pv" >&2
exit 1
