provider "google" {
  project = var.project_id
}

resource "google_storage_bucket" "corpus" {
  name                        = "${var.project_id}-lighthouse-exp015"
  project                     = var.project_id
  location                    = var.location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  labels = {
    application = "lighthouse"
    experiment  = "015"
    purpose     = "temporary-benchmark-corpus"
  }

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age = var.delete_after_days
    }

    action {
      type = "Delete"
    }
  }
}
