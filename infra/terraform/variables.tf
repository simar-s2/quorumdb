variable "project_id" {
  description = "GCP project to create everything in."
  type        = string
}

variable "region" {
  description = "Region for the cluster. The three QuorumDB nodes go in the region's first three zones."
  type        = string
  default     = "us-west1"
}
