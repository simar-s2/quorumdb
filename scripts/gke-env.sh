# Settings shared by the GKE scripts (sourced, not run). Override any of them in the environment.
PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}
REGION=${REGION:-us-west1}
NAMESPACE=quorumdb
REGISTRY=${REGISTRY:-$REGION-docker.pkg.dev/$PROJECT_ID/quorumdb}
# The kubectl context that `gcloud container clusters get-credentials` creates. Every kubectl call in
# these scripts names it, so they can never act on another cluster by accident.
KUBE_CONTEXT=${KUBE_CONTEXT:-gke_${PROJECT_ID}_${REGION}_quorumdb}

if [ -z "$PROJECT_ID" ]; then
  echo "no GCP project: run 'gcloud config set project <id>' or set PROJECT_ID" >&2
  exit 1
fi

kube() { kubectl --context "$KUBE_CONTEXT" "$@"; }
