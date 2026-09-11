variable "project_id" {
  description = "GCP project that owns the benchmark corpus bucket."
  type        = string
}

variable "location" {
  description = "Single region in which the corpus and benchmark run."
  type        = string
  default     = "US-CENTRAL1"
}

variable "delete_after_days" {
  description = "Maximum age of temporary corpus objects."
  type        = number
  default     = 14

  validation {
    condition     = var.delete_after_days >= 1 && var.delete_after_days <= 30
    error_message = "Temporary corpus retention must be between 1 and 30 days."
  }
}
