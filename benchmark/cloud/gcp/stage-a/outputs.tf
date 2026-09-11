output "database_name" {
  value = google_compute_instance.database.name
}

output "database_internal_ip" {
  value = google_compute_instance.database.network_interface[0].network_ip
}

output "loadgen_name" {
  value = google_compute_instance.loadgen.name
}

output "loadgen_internal_ip" {
  value = google_compute_instance.loadgen.network_interface[0].network_ip
}

output "automatic_delete_after_seconds" {
  value = var.max_run_seconds
}
