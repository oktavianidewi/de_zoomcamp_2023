terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "data-lake-bucket" {
  name                        = "${local.data_lake_bucket}_${var.project}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
  force_destroy = true
}
