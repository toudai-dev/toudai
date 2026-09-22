output "watchdog_function_name" {
  value = aws_lambda_function.watchdog.function_name
}

output "watchdog_schedule" {
  description = "EventBridge rule the watchdog runs on; it terminates any tagged instance older than the declared lifetime."
  value       = aws_cloudwatch_event_rule.watchdog.schedule_expression
}

output "watchdog_max_lifetime_hours" {
  value = var.max_lifetime_hours
}

output "budget_name" {
  value = aws_budgets_budget.stage_b.name
}

output "budget_ceiling_usd" {
  value = var.budget_ceiling_usd
}

output "budget_topic_arn" {
  description = "SNS topic budget alerts are published to; subscribe an address here to receive them."
  value       = aws_sns_topic.budget.arn
}
