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

locals {
  prefix = "lighthouse-exp015-stage-b"

  common_tags = {
    experiment = "015"
    stage      = "b"
  }

  max_lifetime_minutes = var.max_lifetime_hours * 60

  # The corpus is immutable for the whole experiment, so nodes may read it and
  # write under `results/`, but nothing in this module can delete or overwrite
  # a validated source batch.
  bucket_arn       = "arn:aws:s3:::${var.corpus_bucket}"
  corpus_objects   = "${local.bucket_arn}/corpus/*"
  manifest_objects = "${local.bucket_arn}/manifests/*"
  result_objects   = "${local.bucket_arn}/results/*"

  leaf_config_b64  = base64encode(file("${path.module}/clickhouse-leaf.xml"))
  users_config_b64 = base64encode(file("${path.module}/clickhouse-users.xml"))

  coordinator_config_b64 = base64encode(templatefile("${path.module}/clickhouse-coordinator.xml.tftpl", {
    leaf_ips = aws_instance.leaf[*].private_ip
  }))
}

data "aws_iam_policy_document" "instance_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${local.prefix}-node"
  description        = "Temporary least-privilege corpus read and benchmark result write access"
  assume_role_policy = data.aws_iam_policy_document.instance_assume_role.json
}

# Session Manager is the only management path: no key pair, no inbound SSH rule,
# and no long-lived credentials on the nodes.
resource "aws_iam_role_policy_attachment" "session_manager" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "node_access" {
  statement {
    sid       = "ReadImmutableCorpus"
    actions   = ["s3:GetObject"]
    resources = [local.corpus_objects, local.manifest_objects]
  }

  statement {
    sid       = "WriteBenchmarkResults"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [local.result_objects]
  }

  statement {
    sid       = "ListBenchmarkPrefixes"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["corpus/*", "manifests/*", "results/*"]
    }
  }
}

resource "aws_iam_role_policy" "node_access" {
  name   = "${local.prefix}-node-access"
  role   = aws_iam_role.node.id
  policy = data.aws_iam_policy_document.node_access.json
}

resource "aws_iam_instance_profile" "node" {
  name = "${local.prefix}-node"
  role = aws_iam_role.node.name
}

resource "aws_instance" "leaf" {
  count = var.shard_count

  ami                         = var.image
  instance_type               = var.leaf_instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  associate_public_ip_address = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  # EC2 has no provider-enforced maximum run duration. Each node schedules its
  # own halt from user data at `max_lifetime_hours`, and this shutdown behaviour
  # makes the hypervisor terminate the instance and delete its root volume, so an
  # abandoned run cannot bill past its declared lifetime.
  instance_initiated_shutdown_behavior = "terminate"

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/startup-wrapper.sh.tftpl", {
    clickhouse_config    = local.leaf_config_b64
    clickhouse_users     = local.users_config_b64
    max_lifetime_minutes = local.max_lifetime_minutes
    extra_env            = ""
    startup_script       = file("${path.module}/startup-leaf.sh")
  })

  tags = merge(local.common_tags, {
    Name     = format("%s-leaf-%02d", local.prefix, count.index + 1)
    role     = "serving-leaf"
    shard    = format("%02d", count.index + 1)
    capacity = "on-demand"
  })

  depends_on = [
    aws_iam_role_policy.node_access,
    aws_iam_role_policy_attachment.session_manager,
  ]
}

resource "aws_instance" "coordinator" {
  ami                         = var.image
  instance_type               = var.coordinator_machine_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  associate_public_ip_address = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  instance_initiated_shutdown_behavior = "terminate"

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/startup-wrapper.sh.tftpl", {
    clickhouse_config    = local.coordinator_config_b64
    clickhouse_users     = local.users_config_b64
    max_lifetime_minutes = local.max_lifetime_minutes
    extra_env            = ""
    startup_script       = file("${path.module}/startup-coordinator.sh")
  })

  tags = merge(local.common_tags, {
    Name     = "${local.prefix}-coordinator"
    role     = "coordinator"
    capacity = "on-demand"
  })

  depends_on = [
    aws_iam_role_policy.node_access,
    aws_iam_role_policy_attachment.session_manager,
  ]
}

resource "aws_instance" "loadgen" {
  ami                         = var.image
  instance_type               = var.loadgen_machine_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  associate_public_ip_address = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  instance_initiated_shutdown_behavior = "terminate"

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  # The load generator needs the endpoint map so its benchmark scripts can be
  # driven without a separate inventory lookup.
  user_data = templatefile("${path.module}/startup-wrapper.sh.tftpl", {
    clickhouse_config    = local.leaf_config_b64
    clickhouse_users     = local.users_config_b64
    max_lifetime_minutes = local.max_lifetime_minutes
    extra_env = join("\n", [
      "export LIGHTHOUSE_COORDINATOR_IP='${aws_instance.coordinator.private_ip}'",
      "export LIGHTHOUSE_LEAF_IPS='${join(",", aws_instance.leaf[*].private_ip)}'",
    ])
    startup_script = file("${path.module}/startup-loadgen.sh")
  })

  tags = merge(local.common_tags, {
    Name     = "${local.prefix}-loadgen"
    role     = "load-generator"
    capacity = "on-demand"
  })

  depends_on = [
    aws_iam_role_policy.node_access,
    aws_iam_role_policy_attachment.session_manager,
  ]
}
