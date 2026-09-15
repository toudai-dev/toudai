variable "region" {
  description = "AWS region that owns the Stage B fleet."
  type        = string
  default     = "us-west-2"
}

variable "max_lifetime_hours" {
  description = "Instance lifetime the watchdog enforces; must match the Stage B topology."
  type        = number
  default     = 36

  validation {
    condition     = var.max_lifetime_hours >= 12 && var.max_lifetime_hours <= 48
    error_message = "Stage B lifetime must be between 12 and 48 hours."
  }
}

variable "watchdog_grace_minutes" {
  description = "Extra time past the lifetime before the watchdog terminates a node, so a healthy node's own shutdown timer fires first."
  type        = number
  default     = 15

  validation {
    condition     = var.watchdog_grace_minutes >= 5 && var.watchdog_grace_minutes <= 120
    error_message = "watchdog_grace_minutes must be between 5 and 120."
  }
}

variable "watchdog_schedule_minutes" {
  description = "How often the watchdog looks for instances that outlived their lifetime."
  type        = number
  default     = 15

  validation {
    condition     = var.watchdog_schedule_minutes >= 5 && var.watchdog_schedule_minutes <= 60
    error_message = "watchdog_schedule_minutes must be between 5 and 60."
  }
}

variable "budget_ceiling_usd" {
  description = "Monthly account-wide cost ceiling; the planned 1-, 2-, and 4-shard Stage B matrix is expected to stay well below it."
  type        = number
  default     = 250

  validation {
    condition     = var.budget_ceiling_usd >= 50
    error_message = "budget_ceiling_usd must be at least 50."
  }
}

variable "budget_alert_email" {
  description = "Address subscribed to budget alerts; no subscription is created when empty."
  type        = string
  default     = ""
}
