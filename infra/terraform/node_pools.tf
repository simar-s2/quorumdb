locals {
  node_config = {
    machine_type = var.machine_type
    disk_type    = "pd-balanced"
    disk_size_gb = 30
  }
}

# The database pool: one node in each of the three zones (in a regional cluster, node_count is per
# zone). The StatefulSet selects this pool by the label GKE puts on its nodes,
# cloud.google.com/gke-nodepool=db.
resource "google_container_node_pool" "db" {
  name           = "db"
  cluster        = google_container_cluster.quorumdb.id
  node_locations = local.zones
  node_count     = 1

  node_config {
    machine_type    = local.node_config.machine_type
    disk_type       = local.node_config.disk_type
    disk_size_gb    = local.node_config.disk_size_gb
    service_account = google_service_account.nodes.email
    # Access is controlled by the service account's IAM roles, so the OAuth scope can be broad.
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

# One node, in the first zone, for the benchmark and chaos clients. Running them here keeps them
# from taking CPU away from a database node, which would distort the measurements.
resource "google_container_node_pool" "tools" {
  name           = "tools"
  cluster        = google_container_cluster.quorumdb.id
  node_locations = [local.zones[0]]
  node_count     = 1

  node_config {
    machine_type    = local.node_config.machine_type
    disk_type       = local.node_config.disk_type
    disk_size_gb    = local.node_config.disk_size_gb
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}
