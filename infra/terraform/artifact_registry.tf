# Docker repository for the quorumdb and tools images. `terraform destroy` deletes it with its images.
resource "google_artifact_registry_repository" "quorumdb" {
  location      = var.region
  repository_id = "quorumdb"
  format        = "DOCKER"
  depends_on    = [google_project_service.enabled]
}
