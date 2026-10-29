variable "project_id" {
  description = "GCP project to create everything in."
  type        = string
}

variable "region" {
  description = "Region for the cluster. The three QuorumDB nodes go in the region's first three zones."
  type        = string
  default     = "us-west1"
}

variable "machine_type" {
  description = "Machine type for every node. e2-standard-2 has 2 vCPUs and 8 GB of memory."
  type        = string
  default     = "e2-standard-2"
}
