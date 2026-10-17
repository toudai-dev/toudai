packer {
  required_plugins {
    googlecompute = {
      source  = "github.com/hashicorp/googlecompute"
      version = "~> 1.2"
    }
  }
}

variable "project_id" {
  type = string
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "network" {
  type = string
}

variable "subnetwork" {
  type = string
}

variable "ssh_username" {
  type    = string
  default = "wuddarwin"
}

variable "ssh_private_key_file" {
  type    = string
  default = "~/.ssh/gcp"
}

source "googlecompute" "benchmark" {
  project_id                  = var.project_id
  zone                        = var.zone
  source_image_family         = "debian-12"
  source_image_project_id     = ["debian-cloud"]
  machine_type                = "e2-standard-4"
  disk_size                   = 100
  disk_type                   = "pd-balanced"
  ssh_username                = var.ssh_username
  ssh_private_key_file        = pathexpand(var.ssh_private_key_file)
  network                     = var.network
  subnetwork                  = var.subnetwork
  tags                        = ["lighthouse-iap-ssh"]
  use_iap                     = true
  max_run_duration_in_seconds = 3600
  instance_termination_action = "DELETE"

  image_name   = "lighthouse-benchmark-${formatdate("YYYYMMDD-hhmmss", timestamp())}"
  image_family = "lighthouse-benchmark"
  image_labels = {
    application = "lighthouse"
    experiment  = "015"
    managed_by  = "packer"
  }

}

build {
  sources = ["source.googlecompute.benchmark"]

  provisioner "file" {
    source      = ".flox/env/manifest.toml"
    destination = "/tmp/toudai-manifest.toml"
  }

  provisioner "file" {
    source      = ".flox/env/manifest.lock"
    destination = "/tmp/toudai-manifest.lock"
  }

  provisioner "file" {
    source      = ".flox/env.json"
    destination = "/tmp/toudai-env.json"
  }

  provisioner "shell" {
    script = "benchmark/cloud/gcp/image/provision.sh"
  }
}
