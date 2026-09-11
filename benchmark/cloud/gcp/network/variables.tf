variable "project_id" {
  description = "GCP project that owns the benchmark resources."
  type        = string
}

variable "region" {
  description = "GCP region for the Stage A benchmark."
  type        = string
  default     = "us-central1"
}

variable "subnet_cidr" {
  description = "Private IPv4 range for benchmark instances."
  type        = string
  default     = "10.142.0.0/24"
}
