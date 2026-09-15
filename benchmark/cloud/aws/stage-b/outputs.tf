output "leaf_instance_ids" {
  value = aws_instance.leaf[*].id
}

output "leaf_names" {
  value = aws_instance.leaf[*].tags["Name"]
}

output "leaf_private_ips" {
  value = aws_instance.leaf[*].private_ip
}

output "coordinator_instance_id" {
  value = aws_instance.coordinator.id
}

output "coordinator_private_ip" {
  value = aws_instance.coordinator.private_ip
}

output "loadgen_instance_id" {
  value = aws_instance.loadgen.id
}

output "loadgen_private_ip" {
  value = aws_instance.loadgen.private_ip
}

output "maximum_lifetime_hours" {
  value = var.max_lifetime_hours
}

# Echoed so an operator or an unattended teardown can destroy with exactly the
# inputs this topology was applied with, without having to remember them.
output "image" {
  value = var.image
}

output "vpc_id" {
  value = var.vpc_id
}

output "subnet_id" {
  value = var.subnet_id
}

output "security_group_id" {
  value = var.security_group_id
}

output "budget_topic_arn" {
  value = var.budget_topic_arn
}

output "session_manager_targets" {
  description = "Instance IDs for `aws ssm start-session`; there is no inbound SSH path."
  value = concat(
    aws_instance.leaf[*].id,
    [aws_instance.coordinator.id, aws_instance.loadgen.id],
  )
}
