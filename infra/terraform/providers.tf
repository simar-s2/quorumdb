terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.0"
    }
  }

  # State is kept in a GCS bucket, so it is not lost with this laptop and is locked during an apply.
  # A backend block cannot use variables, so `make up` passes the bucket name with -backend-config.
  backend "gcs" {
    prefix = "quorumdb"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
