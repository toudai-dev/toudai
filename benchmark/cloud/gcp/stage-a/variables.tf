variable "project_id" {
  description = "GCP project that owns the Stage A benchmark resources."
  type        = string
  default     = "lighthouse-508219"
}

variable "region" {
  description = "GCP region for the Stage A benchmark."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the Stage A benchmark."
  type        = string
  default     = "us-central1-a"
}

variable "network" {
  description = "Existing private benchmark VPC."
  type        = string
  default     = "lighthouse-benchmark"
}

variable "subnetwork" {
  description = "Existing private benchmark subnet."
  type        = string
  default     = "lighthouse-benchmark-us-central1"
}

variable "image" {
  description = "Explicitly validated dependency image used by both nodes; the first Stage A image contains incompatible ClickHouse 26.3 and must not be reused."
  type        = string
}

variable "corpus_bucket" {
  description = "Private bucket containing the immutable benchmark corpus."
  type        = string
  default     = "lighthouse-508219-lighthouse-exp015"
}

variable "database_machine_type" {
  description = "Storage-optimized Stage A ClickHouse leaf."
  type        = string
  default     = "z3-highmem-14-standardlssd"
}

variable "loadgen_machine_type" {
  description = "Dedicated query and ingestion driver."
  type        = string
  default     = "e2-standard-4"
}

variable "max_run_seconds" {
  description = "Provider-enforced lifetime before each VM is deleted."
  type        = number
  default     = 21600

  validation {
    condition     = var.max_run_seconds >= 900 && var.max_run_seconds <= 21600
    error_message = "Stage A VM lifetime must be between 15 minutes and 6 hours."
  }
}
