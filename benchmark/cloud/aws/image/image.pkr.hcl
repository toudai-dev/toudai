packer {
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = "~> 1.7"
    }
  }
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "availability_zone" {
  type    = string
  default = "us-west-2a"
}

variable "instance_type" {
  description = "Disposable builder shape; the image carries no workload."
  type        = string
  default     = "m7i.large"
}

variable "ssh_username" {
  description = "Default login user of the official Debian AWS AMI."
  type        = string
  default     = "admin"
}

source "amazon-ebs" "benchmark" {
  region        = var.region
  instance_type = var.instance_type
  ssh_username  = var.ssh_username

  ami_name        = "lighthouse-benchmark-{{isotime \"20060102-150405\"}}"
  ami_description = "Lighthouse Experiment 015 benchmark dependencies with the Flox-pinned ClickHouse 26.8.2.7 closure"

  source_ami_filter {
    filters = {
      name                = "debian-12-amd64-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
      architecture        = "x86_64"
    }
    owners      = ["136693071363"]
    most_recent = true
  }

  # The builder needs package mirrors, so it runs in the default VPC's public
  # subnet. Nothing else uses that network and the builder is deleted after the
  # build; only the operator's public address may reach its temporary SSH rule.
  vpc_filter {
    filters = {
      "isDefault" = "true"
    }
  }

  subnet_filter {
    filters = {
      "availability-zone" = var.availability_zone
    }
    most_free = true
  }

  associate_public_ip_address               = true
  temporary_security_group_source_public_ip = true

  launch_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = 100
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    application = "lighthouse"
    experiment  = "015"
    managed_by  = "packer"
  }

  run_tags = {
    application = "lighthouse"
    experiment  = "015"
    managed_by  = "packer"
  }
}

build {
  sources = ["source.amazon-ebs.benchmark"]

  provisioner "file" {
    source      = ".flox/env/manifest.toml"
    destination = "/tmp/lighthouse-manifest.toml"
  }

  provisioner "file" {
    source      = ".flox/env/manifest.lock"
    destination = "/tmp/lighthouse-manifest.lock"
  }

  provisioner "file" {
    source      = ".flox/env.json"
    destination = "/tmp/lighthouse-env.json"
  }

  provisioner "shell" {
    script = "benchmark/cloud/aws/image/provision.sh"
  }
}
