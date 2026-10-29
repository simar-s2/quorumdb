# A dedicated service account for the nodes, with only the access they need, instead of the default
# Compute Engine account (which often has broad Editor access to the whole project).
resource "google_service_account" "nodes" {
  account_id   = "quorumdb-nodes"
  display_name = "QuorumDB GKE nodes"
}

# GKE's minimum role for nodes: write logs and metrics.
resource "google_project_iam_member" "nodes_default_role" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = google_service_account.nodes.member
}

# Pull images, from this one repository only.
resource "google_artifact_registry_repository_iam_member" "nodes_pull_images" {
  location   = google_artifact_registry_repository.quorumdb.location
  repository = google_artifact_registry_repository.quorumdb.name
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.nodes.member
}
