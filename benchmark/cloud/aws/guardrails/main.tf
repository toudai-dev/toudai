provider "aws" {
  region = var.region

  default_tags {
    tags = {
      application = "lighthouse"
      experiment  = "015"
      stage       = "b"
      managed_by  = "opentofu"
    }
  }
}

# GCP's `max_run_duration` has no EC2 equivalent, so the lifetime ceiling is
# enforced from outside the fleet. Every tagged instance older than the declared
# lifetime plus a grace period is terminated, whatever state its operating
# system is in. The tag filters are the whole safety story: they are also
# enforced by IAM, so the watchdog cannot terminate anything outside the
# experiment even if its own logic were wrong.
locals {
  prefix = "lighthouse-exp015-stage-b"

  watchdog_tags = {
    application = "lighthouse"
    experiment  = "015"
    stage       = "b"
    managed_by  = "opentofu"
  }

  watchdog_tag_filters = join(",", [
    for key, value in local.watchdog_tags : "${key}=${value}"
  ])
}

data "archive_file" "watchdog" {
  type        = "zip"
  source_dir  = "${path.module}/watchdog"
  output_path = "${path.module}/.terraform/watchdog.zip"
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "watchdog" {
  name               = "${local.prefix}-watchdog"
  description        = "Terminates Stage B instances that outlive their declared lifetime"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "watchdog_logs" {
  role       = aws_iam_role.watchdog.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "watchdog_terminate" {
  statement {
    sid       = "FindTaggedBenchmarkInstances"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }

  statement {
    sid       = "TerminateTaggedBenchmarkInstances"
    actions   = ["ec2:TerminateInstances"]
    resources = ["arn:aws:ec2:*:*:instance/*"]

    # EC2 has no resource-level tag condition for most APIs, but instance
    # termination does honour `ec2:ResourceTag`, so the experiment tag doubles
    # as the blast-radius limit rather than relying on the handler's filters.
    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/managed_by"
      values   = ["opentofu"]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/experiment"
      values   = ["015"]
    }
  }
}

resource "aws_iam_role_policy" "watchdog_terminate" {
  name   = "${local.prefix}-watchdog-terminate"
  role   = aws_iam_role.watchdog.id
  policy = data.aws_iam_policy_document.watchdog_terminate.json
}

resource "aws_cloudwatch_log_group" "watchdog" {
  name              = "/aws/lambda/${local.prefix}-watchdog"
  retention_in_days = 14
}

resource "aws_lambda_function" "watchdog" {
  function_name    = "${local.prefix}-watchdog"
  description      = "Provider-side lifetime ceiling for the Stage B benchmark fleet"
  role             = aws_iam_role.watchdog.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 128
  filename         = data.archive_file.watchdog.output_path
  source_code_hash = data.archive_file.watchdog.output_base64sha256

  environment {
    variables = {
      MAX_LIFETIME_HOURS = tostring(var.max_lifetime_hours)
      GRACE_MINUTES      = tostring(var.watchdog_grace_minutes)
      TAG_FILTERS        = local.watchdog_tag_filters
    }
  }

  depends_on = [aws_cloudwatch_log_group.watchdog]
}

resource "aws_cloudwatch_event_rule" "watchdog" {
  name                = "${local.prefix}-watchdog"
  description         = "Terminates Stage B instances that outlive their declared lifetime"
  schedule_expression = "rate(${var.watchdog_schedule_minutes} minutes)"
}

resource "aws_cloudwatch_event_target" "watchdog" {
  rule      = aws_cloudwatch_event_rule.watchdog.name
  target_id = "watchdog"
  arn       = aws_lambda_function.watchdog.arn
}

resource "aws_lambda_permission" "watchdog" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.watchdog.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.watchdog.arn
}

resource "aws_sns_topic" "budget" {
  name = "${local.prefix}-budget"
}

resource "aws_sns_topic_subscription" "budget_email" {
  count     = var.budget_alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.budget.arn
  protocol  = "email"
  endpoint  = var.budget_alert_email
}

# The ceiling is account-wide on purpose: cost allocation tags need 24 hours to
# activate, which is longer than a Stage B topology lives, so a tag-filtered
# budget would not be reliable for the runs it is meant to guard.
resource "aws_budgets_budget" "stage_b" {
  name         = "${local.prefix}-ceiling"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_ceiling_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 50
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"
    subscriber_sns_topic_arns = [aws_sns_topic.budget.arn]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.budget.arn]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.budget.arn]
  }
}

# The cost-ceiling stop action lives in the Stage B topology module: AWS Budgets
# SSM actions require explicit instance IDs and support no tag targeting, so the
# action can only be created by the module that owns those IDs.
