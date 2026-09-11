provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
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
  account_id   = "lighthouse-exp015"
  display_name = "Lighthouse Experiment 015"
  description  = "Temporary read-only corpus access for Stage A benchmark VMs"
}

resource "google_storage_bucket_iam_member" "corpus_reader" {
  bucket = var.corpus_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.benchmark.email}"
}

resource "google_compute_firewall" "loadgen_to_clickhouse" {
  name      = "lighthouse-exp015-loadgen-to-clickhouse"
  network   = data.google_compute_network.benchmark.name
  direction = "INGRESS"
  priority  = 1000

  source_tags = ["lighthouse-exp015-loadgen"]
  target_tags = ["lighthouse-exp015-database"]

  allow {
    protocol = "tcp"
    ports    = ["8123", "9000"]
  }
}

resource "google_compute_instance" "database" {
  name         = "lighthouse-exp015-database"
  machine_type = var.database_machine_type
  zone         = var.zone
  tags         = ["lighthouse-iap-ssh", "lighthouse-exp015-database"]

  labels = {
    application = "lighthouse"
    experiment  = "015"
    purpose     = "stage-a-database"
    capacity    = "spot"
  }

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
    provisioning_model          = "SPOT"
    preemptible                 = true
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "DELETE"

    // This provider-enforced deletion is a cost guardrail even if the driver dies.
    max_run_duration {
      seconds = var.max_run_seconds
    }
  }

  service_account {
    email  = google_service_account.benchmark.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    block-project-ssh-keys = "false"
  }

  metadata_startup_script = file("${path.module}/startup-database.sh")

  deletion_protection = false

  depends_on = [google_storage_bucket_iam_member.corpus_reader]
}

resource "google_compute_instance" "loadgen" {
  name         = "lighthouse-exp015-loadgen"
  machine_type = var.loadgen_machine_type
  zone         = var.zone
  tags         = ["lighthouse-iap-ssh", "lighthouse-exp015-loadgen"]

  labels = {
    application = "lighthouse"
    experiment  = "015"
    purpose     = "stage-a-loadgen"
    capacity    = "spot"
  }

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
    provisioning_model          = "SPOT"
    preemptible                 = true
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "DELETE"

    // This provider-enforced deletion is a cost guardrail even if the driver dies.
    max_run_duration {
      seconds = var.max_run_seconds
    }
  }

  service_account {
    email  = google_service_account.benchmark.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    block-project-ssh-keys = "false"
    database-internal-ip   = google_compute_instance.database.network_interface[0].network_ip
  }

  metadata_startup_script = file("${path.module}/startup-loadgen.sh")

  deletion_protection = false

  depends_on = [google_storage_bucket_iam_member.corpus_reader]
}
