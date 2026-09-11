provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_compute_network" "benchmark" {
  name                    = "lighthouse-benchmark"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "benchmark" {
  name                     = "lighthouse-benchmark-${var.region}"
  ip_cidr_range            = var.subnet_cidr
  network                  = google_compute_network.benchmark.id
  private_ip_google_access = true
  region                   = var.region
}

// IAP is the only inbound SSH path; ClickHouse ports are never internet-facing.
resource "google_compute_firewall" "iap_ssh" {
  name      = "lighthouse-benchmark-iap-ssh"
  network   = google_compute_network.benchmark.name
  direction = "INGRESS"
  priority  = 1000

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["lighthouse-iap-ssh"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
