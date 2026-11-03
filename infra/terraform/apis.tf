# The Google Cloud APIs this setup uses. They stay enabled on destroy so nothing else in the project breaks.
resource "google_project_service" "enabled" {
  for_each = toset([
    "compute.googleapis.com",
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",                  # creates the nodes' service account
    "cloudresourcemanager.googleapis.com", # gives that account its project role
  ])
  service            = each.value
  disable_on_destroy = false
}
