output "image_repository" {
  description = "Where scripts/push-image.sh pushes the images."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.quorumdb.repository_id}"
}

output "cluster_name" {
  value = google_container_cluster.quorumdb.name
}

output "zones" {
  description = "The three zones the QuorumDB nodes run in."
  value       = local.zones
}
