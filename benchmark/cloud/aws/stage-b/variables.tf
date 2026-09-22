variable "region" {
  description = "AWS region that owns the Stage B fleet."
  type        = string
  default     = "us-west-2"
}

variable "vpc_id" {
  description = "Existing private benchmark VPC from the network module."
  type        = string
}

variable "subnet_id" {
  description = "Existing benchmark subnet; every node shares one AZ so no traffic is billed cross-AZ."
  type        = string
}

variable "security_group_id" {
  description = "Existing benchmark security group; ClickHouse ports are reachable only inside it."
  type        = string
}

variable "image" {
  description = "Boot-tested Stage B AMI carrying the Flox-pinned ClickHouse 26.8.2.7 closure."
  type        = string

  validation {
    condition     = can(regex("^ami-[0-9a-f]{8,17}$", var.image))
    error_message = "image must be an AMI ID, for example ami-0abc1234def567890."
  }
}

variable "corpus_bucket" {
  description = "Private bucket holding the immutable corpus, its manifests, and benchmark results."
  type        = string
  default     = "lighthouse-exp015-benchmark-828361646739"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.corpus_bucket))
    error_message = "corpus_bucket must be a valid S3 bucket name."
  }
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

variable "leaf_instance_type" {
  description = "Serving shard shape; requires instance-store NVMe for the disposable ClickHouse data path."
  type        = string
  default     = "i4i.4xlarge"

  # The accepted one-shard baseline left ~31.8 GiB of derived parts on top of
  # 1,400.6 GiB of hot parts, with a 108.3 GiB largest part and a 150 GiB merge
  # cap. A leaf therefore has to hold ~1,582 GiB before headroom, which rules
  # out the 1,875 GB i7i.2xlarge once a 10 % reserve is kept.
  validation {
    condition = contains([
      "i4i.2xlarge", "i4i.4xlarge", "i4i.8xlarge",
      "i7i.2xlarge", "i7i.4xlarge", "i7i.8xlarge",
    ], var.leaf_instance_type)
    error_message = "leaf_instance_type must be a Nitro instance type with local NVMe storage."
  }
}

variable "coordinator_instance_type" {
  description = "Query coordinator shape; it holds no corpus data and only reduces leaf results."
  type        = string
  default     = "m7i.xlarge"
}

variable "loadgen_instance_type" {
  description = "Load generator shape; it only drives HTTP queries at the coordinator."
  type        = string
  default     = "m7i.4xlarge"
}

variable "root_volume_size_gb" {
  description = "gp3 root volume; leaves keep ClickHouse data on instance store, not here."
  type        = number
  default     = 100

  validation {
    condition     = var.root_volume_size_gb >= 30 && var.root_volume_size_gb <= 500
    error_message = "root_volume_size_gb must be between 30 and 500."
  }
}

variable "max_lifetime_hours" {
  description = "Maximum instance lifetime. EC2 has no provider-enforced run duration, so each node schedules its own halt at this age and terminates on shutdown."
  type        = number
  default     = 36

  validation {
    condition     = var.max_lifetime_hours >= 12 && var.max_lifetime_hours <= 48
    error_message = "Stage B lifetime must be between 12 and 48 hours."
  }
}

variable "budget_name" {
  description = "Name of the account-level budget the stop action attaches to; must match the guardrails module."
  type        = string
  default     = "lighthouse-exp015-stage-b-ceiling"
}

variable "budget_topic_arn" {
  description = "SNS topic the stop action notifies, from the guardrails module. No stop action is created when empty."
  type        = string
  default     = ""
}

variable "budget_stop_action_enabled" {
  description = "Whether the budget stops this fleet at the cost ceiling. Disable if AWS Budgets rejects the SSM action."
  type        = bool
  default     = true
}
