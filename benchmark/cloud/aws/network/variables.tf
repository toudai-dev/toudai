variable "region" {
  description = "AWS region for the benchmark network."
  type        = string
  default     = "us-west-2"
}

variable "availability_zone" {
  description = "Single availability zone for every node; cross-AZ traffic is billed."
  type        = string
  default     = "us-west-2a"
}

variable "vpc_cidr" {
  description = "CIDR block for the benchmark VPC."
  type        = string
  default     = "10.60.0.0/16"
}

variable "subnet_cidr" {
  description = "CIDR block for the single public subnet."
  type        = string
  default     = "10.60.1.0/24"
}
