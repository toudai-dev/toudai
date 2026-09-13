provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

locals {
  prefix               = "lighthouse-exp015-stage-b"
  max_lifetime_seconds = var.max_lifetime_hours * 3600
  common_labels = {
    application = "lighthouse"
    experiment  = "015"
    stage       = "b"
  }
}

data "google_compute_image" "benchmark" {
  name    = var.image
  project = var.project_id
}

data "google_compute_network" "benchmark" {
  name    = var.network
  project = var.project_id
}

data "google_compute_subnetwork" "benchmark" {
  name    = var.subnetwork
  project = var.project_id
  region  = var.region
}

resource "google_service_account" "benchmark" {
  account_id   = "lighthouse-exp015-b"
  display_name = "Lighthouse Experiment 015 Stage B"
  description  = "Temporary least-privilege corpus and result object access"
}

resource "google_storage_bucket_iam_member" "object_viewer" {
  bucket = var.corpus_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.benchmark.email}"
}

resource "google_storage_bucket_iam_member" "object_creator" {
  bucket = var.corpus_bucket
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.benchmark.email}"
}

resource "google_compute_firewall" "loadgen_to_coordinator" {
  name      = "${local.prefix}-loadgen-coordinator"
  network   = data.google_compute_network.benchmark.name
  direction = "INGRESS"
  priority  = 1000

  source_tags = ["${local.prefix}-loadgen"]
  target_tags = ["${local.prefix}-coordinator"]

  allow {
    protocol = "tcp"
    ports    = ["8123", "9000"]
  }
}

resource "google_compute_firewall" "drivers_to_leaves" {
  name      = "${local.prefix}-drivers-leaves"
  network   = data.google_compute_network.benchmark.name
  direction = "INGRESS"
  priority  = 1000

  source_tags = ["${local.prefix}-coordinator", "${local.prefix}-loadgen"]
  target_tags = ["${local.prefix}-leaf"]

  allow {
    protocol = "tcp"
    ports    = ["8123", "9000"]
  }
}

resource "google_compute_instance" "leaf" {
  count        = var.shard_count
  name         = format("%s-leaf-%02d", local.prefix, count.index + 1)
  machine_type = var.leaf_machine_type
  zone         = var.zone
  tags         = ["lighthouse-iap-ssh", "${local.prefix}-leaf"]
  labels       = merge(local.common_labels, { purpose = "serving-leaf", capacity = "on-demand" })

  boot_disk {
    auto_delete = true
    initialize_params {
      image = data.google_compute_image.benchmark.self_link
      size  = 100
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.benchmark.self_link
  }

  scheduling {
    provisioning_model          = "STANDARD"
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "DELETE"
    max_run_duration { seconds = local.max_lifetime_seconds }
  }

  service_account {
    email  = google_service_account.benchmark.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    block-project-ssh-keys = "false"
    clickhouse-config      = base64encode(file("${path.module}/clickhouse-leaf.xml"))
    clickhouse-users       = base64encode(file("${path.module}/clickhouse-users.xml"))
  }
  metadata_startup_script = file("${path.module}/startup-leaf.sh")
  deletion_protection     = false

  lifecycle {
    # Z3 integrated Titanium SSD is provider-populated and disposable.
    ignore_changes = [scratch_disk]
  }

  depends_on = [google_storage_bucket_iam_member.object_viewer, google_storage_bucket_iam_member.object_creator]
}

resource "google_compute_instance" "coordinator" {
  name         = "${local.prefix}-coordinator"
  machine_type = var.coordinator_machine_type
  zone         = var.zone
  tags         = ["lighthouse-iap-ssh", "${local.prefix}-coordinator"]
  labels       = merge(local.common_labels, { purpose = "coordinator", capacity = "on-demand" })

  boot_disk {
    auto_delete = true
    initialize_params {
      image = data.google_compute_image.benchmark.self_link
      size  = 100
      type  = "pd-balanced"
    }
  }
  network_interface { subnetwork = data.google_compute_subnetwork.benchmark.self_link }
  scheduling {
    provisioning_model          = "STANDARD"
    automatic_restart           = false
    on_host_maintenance         = "MIGRATE"
    instance_termination_action = "DELETE"
    max_run_duration { seconds = local.max_lifetime_seconds }
  }
  service_account {
    email  = google_service_account.benchmark.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
  metadata = {
    block-project-ssh-keys = "false"
    clickhouse-config = base64encode(templatefile("${path.module}/clickhouse-coordinator.xml.tftpl", {
      leaf_ips = google_compute_instance.leaf[*].network_interface[0].network_ip
    }))
    clickhouse-users = base64encode(file("${path.module}/clickhouse-users.xml"))
  }
  metadata_startup_script = file("${path.module}/startup-coordinator.sh")
  deletion_protection     = false
  depends_on              = [google_storage_bucket_iam_member.object_viewer, google_storage_bucket_iam_member.object_creator]
}

resource "google_compute_instance" "loadgen" {
  name         = "${local.prefix}-loadgen"
  machine_type = var.loadgen_machine_type
  zone         = var.zone
  tags         = ["lighthouse-iap-ssh", "${local.prefix}-loadgen"]
  labels       = merge(local.common_labels, { purpose = "load-generator", capacity = "on-demand" })

  boot_disk {
    auto_delete = true
    initialize_params {
      image = data.google_compute_image.benchmark.self_link
      size  = 100
      type  = "pd-balanced"
    }
  }
  network_interface { subnetwork = data.google_compute_subnetwork.benchmark.self_link }
  scheduling {
    provisioning_model          = "STANDARD"
    automatic_restart           = false
    on_host_maintenance         = "MIGRATE"
    instance_termination_action = "DELETE"
    max_run_duration { seconds = local.max_lifetime_seconds }
  }
  service_account {
    email  = google_service_account.benchmark.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
  metadata = {
    block-project-ssh-keys = "false"
    coordinator-ip         = google_compute_instance.coordinator.network_interface[0].network_ip
    leaf-ips               = join(",", google_compute_instance.leaf[*].network_interface[0].network_ip)
  }
  metadata_startup_script = file("${path.module}/startup-loadgen.sh")
  deletion_protection     = false
  depends_on              = [google_storage_bucket_iam_member.object_viewer, google_storage_bucket_iam_member.object_creator]
}
