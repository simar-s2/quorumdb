#!/usr/bin/env bash
# Builds the server and tools images for linux/amd64 (the CPU type of the GKE nodes) and pushes them
# to Artifact Registry. The tag is the git commit, so a deploy always names the exact code it runs.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/gke-env.sh

tag=$(git rev-parse --short HEAD)
if [ -n "$(git status --porcelain)" ]; then
  # Uncommitted changes: add a timestamp so the nodes never reuse an older image with the same tag.
  tag="$tag-dirty-$(date +%Y%m%d%H%M%S)"
fi

# Lets docker push with your gcloud login (adds a credential helper to ~/.docker/config.json).
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

docker buildx build --platform linux/amd64 --target server --tag "$REGISTRY/quorumdb:$tag" --push .
docker buildx build --platform linux/amd64 --target tools --tag "$REGISTRY/tools:$tag" --push .

# The deploy script reads the tag from here.
mkdir -p .deploy
echo "$tag" > .deploy/image-tag
echo "pushed $REGISTRY/quorumdb:$tag and $REGISTRY/tools:$tag"
