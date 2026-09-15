# Cost-ceiling stop action for the fleet this module creates.
#
# AWS Budgets SSM actions take explicit instance IDs and support no tag
# targeting, so this cannot live with the account-level budget in the guardrails
# module. The role is still tag-conditioned, which means that even if the
# instance list were wrong, the action could not stop anything outside the
# experiment.
#
# Stopping is deliberately weaker than the watchdog's termination: at the
# ceiling the run is already lost, and a stopped instance stays recoverable for
# forensics while no longer billing compute.

locals {
  budget_action_instance_ids = concat(
    aws_instance.leaf[*].id,
    [aws_instance.coordinator.id, aws_instance.loadgen.id],
  )

  # Without a topic there is no notification target for the action, so the
  # action is only created when one is supplied.
  budget_action_enabled = var.budget_stop_action_enabled && var.budget_topic_arn != ""
}

data "aws_iam_policy_document" "budget_action_assume_role" {
  count = local.budget_action_enabled ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "budget_action" {
  count              = local.budget_action_enabled ? 1 : 0
  name               = "${local.prefix}-budget-action"
  description        = "Lets AWS Budgets stop this Stage B fleet at the cost ceiling"
  assume_role_policy = data.aws_iam_policy_document.budget_action_assume_role[0].json
}

data "aws_iam_policy_document" "budget_action" {
  count = local.budget_action_enabled ? 1 : 0

  statement {
    sid       = "FindTaggedBenchmarkInstances"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }

  statement {
    sid       = "StopTaggedBenchmarkInstances"
    actions   = ["ec2:StopInstances"]
    resources = ["arn:aws:ec2:*:*:instance/*"]

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

  statement {
    sid       = "RunStopDocument"
    actions   = ["ssm:GetDocument", "ssm:DescribeDocument"]
    resources = ["arn:aws:ssm:*::document/AWS-StopEC2Instance"]
  }
}

resource "aws_iam_role_policy" "budget_action" {
  count  = local.budget_action_enabled ? 1 : 0
  name   = "${local.prefix}-budget-action"
  role   = aws_iam_role.budget_action[0].id
  policy = data.aws_iam_policy_document.budget_action[0].json
}

resource "aws_budgets_budget_action" "stop_instances" {
  count = local.budget_action_enabled ? 1 : 0

  budget_name        = var.budget_name
  action_type        = "RUN_SSM_DOCUMENTS"
  approval_model     = "AUTOMATIC"
  notification_type  = "ACTUAL"
  execution_role_arn = aws_iam_role.budget_action[0].arn

  action_threshold {
    action_threshold_type  = "PERCENTAGE"
    action_threshold_value = 100
  }

  definition {
    ssm_action_definition {
      action_sub_type = "STOP_EC2_INSTANCES"
      instance_ids    = local.budget_action_instance_ids
      region          = var.region
    }
  }

  subscriber {
    address           = var.budget_topic_arn
    subscription_type = "SNS"
  }

  depends_on = [aws_iam_role_policy.budget_action]
}
