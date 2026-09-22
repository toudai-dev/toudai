output "vpc_id" {
  value = aws_vpc.benchmark.id
}

output "subnet_id" {
  value = aws_subnet.benchmark.id
}

output "security_group_id" {
  value = aws_security_group.benchmark.id
}

output "availability_zone" {
  value = var.availability_zone
}
