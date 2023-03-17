terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "PCT-Air-Quality" {

  bucket = "PCT-Air-Quality"

  versioning = {
    enabled = true
  }


}

resource "aws_redshift_cluster" "PCT-Cluster" {
  cluster_identifier = "pct-cluster"
  database_name      = "pct_air_quality"
  master_username    = "admin"
  master_password    = "6005Oakbury!"
  node_type          = "dc2.large"
  cluster_type       = "single-node"

  skip_final_snapshot                 = true
  publicly_accessible                 = true
  automated_snapshot_retention_period = 0
}
