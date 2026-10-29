resource "google_compute_network" "quorumdb" {
  name                    = "quorumdb"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.enabled]
}

# One subnet for the region (a subnet spans every zone in it). The nodes get IPs from the primary
# range; pods and Services get theirs from the two secondary ranges. This makes the cluster
# VPC-native, so pod IPs are routable in the VPC. Dataplane V2 requires it.
resource "google_compute_subnetwork" "quorumdb" {
  name          = "quorumdb-${var.region}"
  region        = var.region
  network       = google_compute_network.quorumdb.id
  ip_cidr_range = "10.0.0.0/20"

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.16.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.20.0.0/20"
  }

  # Nodes reach Google APIs (Artifact Registry, Cloud Logging) over Google's internal network.
  private_ip_google_access = true
}
