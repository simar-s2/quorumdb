# Settings shared by the GKE scripts (sourced, not run). Override any of them in the environment.
PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}
REGION=${REGION:-us-west1}
NAMESPACE=quorumdb
REGISTRY="$REGION-docker.pkg.dev/$PROJECT_ID/quorumdb"

if [ -z "$PROJECT_ID" ]; then
  echo "no GCP project: run 'gcloud config set project <id>' or set PROJECT_ID" >&2
  exit 1
fi
