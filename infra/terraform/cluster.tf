# The region's first three zones (us-west1-a, -b and -c by default). Each gets one QuorumDB node.
data "google_compute_zones" "available" {
  region     = var.region
  depends_on = [google_project_service.enabled]
}

locals {
  zones = slice(sort(data.google_compute_zones.available.names), 0, 3)
}

resource "google_container_cluster" "quorumdb" {
  name = "quorumdb"
  # A region (not a zone) as the location makes this a regional cluster: GKE runs a replica of the
  # control plane in each zone, so losing one zone does not take down the Kubernetes API.
  location       = var.region
  node_locations = local.zones

  network    = google_compute_network.quorumdb.id
  subnetwork = google_compute_subnetwork.quorumdb.id
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # GKE Dataplane V2: eBPF networking built on Cilium, with NetworkPolicy enforcement built in.
  # The chaos test relies on it to cut links between zones.
  datapath_provider = "ADVANCED_DATAPATH"

  # GKE creates a default node pool with the cluster. Delete it right away and use the pools in
  # node_pools.tf, which Terraform can change without recreating the cluster. Provider 8 marks this
  # deprecated because GKE 1.36.4+ can skip the default pool entirely, but this way works on every
  # GKE version the release channel might pick.
  remove_default_node_pool = true
  initial_node_count       = 1

  release_channel {
    channel = "REGULAR"
  }

  # Allow `terraform destroy` (make down) to delete the cluster.
  deletion_protection = false
}
