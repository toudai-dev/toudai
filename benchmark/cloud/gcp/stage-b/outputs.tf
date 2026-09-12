output "leaf_names" { value = google_compute_instance.leaf[*].name }
output "leaf_internal_ips" { value = google_compute_instance.leaf[*].network_interface[0].network_ip }
output "coordinator_name" { value = google_compute_instance.coordinator.name }
output "coordinator_internal_ip" { value = google_compute_instance.coordinator.network_interface[0].network_ip }
output "loadgen_name" { value = google_compute_instance.loadgen.name }
output "loadgen_internal_ip" { value = google_compute_instance.loadgen.network_interface[0].network_ip }
output "maximum_lifetime_hours" { value = var.max_lifetime_hours }
