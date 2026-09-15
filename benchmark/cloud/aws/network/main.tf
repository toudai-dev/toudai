provider "aws" {
  region = var.region

  default_tags {
    tags = {
      application = "lighthouse"
      experiment  = "015"
      managed_by  = "opentofu"
    }
  }
}

locals {
  prefix = "lighthouse-exp015"
}

resource "aws_vpc" "benchmark" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${local.prefix}-vpc" }
}

# A public subnet exists only so instances can reach SSM and package mirrors
# without a NAT gateway, which costs more per hour than this fleet's entire
# management traffic is worth. No inbound rule permits internet access.
resource "aws_subnet" "benchmark" {
  vpc_id                  = aws_vpc.benchmark.id
  cidr_block              = var.subnet_cidr
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = { Name = "${local.prefix}-subnet" }
}

resource "aws_internet_gateway" "benchmark" {
  vpc_id = aws_vpc.benchmark.id

  tags = { Name = "${local.prefix}-igw" }
}

resource "aws_route_table" "benchmark" {
  vpc_id = aws_vpc.benchmark.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.benchmark.id
  }

  tags = { Name = "${local.prefix}-rt" }
}

resource "aws_route_table_association" "benchmark" {
  subnet_id      = aws_subnet.benchmark.id
  route_table_id = aws_route_table.benchmark.id
}

# Every node shares one security group. ClickHouse ports are reachable only from
# another member of the group, so no database port is internet-facing, and
# management access is Session Manager, which needs no inbound rule at all.
resource "aws_security_group" "benchmark" {
  name        = "${local.prefix}-nodes"
  description = "Lighthouse benchmark nodes; ClickHouse reachable only inside the group"
  vpc_id      = aws_vpc.benchmark.id

  ingress {
    description = "ClickHouse HTTP and native from other benchmark nodes"
    from_port   = 8123
    to_port     = 9000
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Intra-group ICMP for reachability checks"
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    self        = true
  }

  egress {
    description = "Session Manager, package mirrors, and S3 over the gateway endpoint"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.prefix}-nodes" }
}

# Gateway endpoints are free and keep corpus reads and result writes off the
# metered internet path.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.benchmark.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.benchmark.id]

  tags = { Name = "${local.prefix}-s3" }
}
