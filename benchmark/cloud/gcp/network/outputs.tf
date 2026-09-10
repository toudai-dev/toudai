output "network" {
  value = google_compute_network.benchmark.name
}

output "subnetwork" {
  value = google_compute_subnetwork.benchmark.name
}
