variable "project_id" {
  description = "GCP project that owns the Stage B resources."
  type        = string
  default     = "lighthouse-508219"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
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
  description = "Boot-tested Stage B image containing ClickHouse 26.8.2.7."
  type        = string
  default     = "lighthouse-benchmark-20260912-172742"

  validation {
    condition     = var.image == "lighthouse-benchmark-20260912-172742"
    error_message = "Stage B must use the validated lighthouse-benchmark-20260912-172742 image."
  }
}

variable "corpus_bucket" {
  description = "Private bucket used for immutable corpus reads and benchmark result writes."
  type        = string
  default     = "lighthouse-508219-lighthouse-exp015"
}

variable "shard_count" {
  description = "Number of one-replica serving shards in this sequential Stage B run."
  type        = number
  default     = 1

  validation {
    condition     = contains([1, 2, 4], var.shard_count)
    error_message = "shard_count must be 1, 2, or 4."
  }
}

variable "leaf_machine_type" {
  type    = string
  default = "z3-highmem-14-standardlssd"
}

variable "coordinator_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "loadgen_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "max_lifetime_hours" {
  description = "Provider-enforced maximum VM lifetime; auto-deleting boot disks die with their VM."
  type        = number
  default     = 36

  validation {
    condition     = var.max_lifetime_hours >= 12 && var.max_lifetime_hours <= 48
    error_message = "Stage B lifetime must be between 12 and 48 hours."
  }
}
