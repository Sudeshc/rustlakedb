# Copyright (C) 2026 Sudeshc
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Production Multi-Petabyte Cloud Object Storage for RustLakeDB v9.0.0:
# - Primary S3 Iceberg v3 Warehouse Bucket + KMS Encryption + Versioning + Public Access Block
# - Cross-Region Replication (CRR) to DR Region for RPO ~ seconds regional failover
# - Dedicated IAM Role for Iceberg REST STS Vended Credentials (`X-Iceberg-Access-Delegation: vended-credentials`)

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.40.0"
    }
  }
}

variable "warehouse_prefix" {
  type        = string
  default     = "rustlakedb-prod-warehouse"
  description = "Globally unique prefix for primary & DR S3 warehouse buckets"
}

variable "primary_region" {
  type    = string
  default = "us-east-1"
}

variable "dr_region" {
  type    = string
  default = "us-west-2"
}

variable "eks_oidc_provider_arn" {
  type        = string
  description = "EKS OIDC Provider ARN for IRSA (RustLakeDB Coordinator/Worker ServiceAccount)"
}

variable "eks_oidc_provider_url" {
  type        = string
  description = "EKS OIDC Provider URL without https://"
}

provider "aws" {
  region = var.primary_region
}

provider "aws" {
  alias  = "dr"
  region = var.dr_region
}

resource "aws_kms_key" "primary" {
  description             = "RustLakeDB Primary Iceberg Warehouse KMS Key"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_key" "dr" {
  provider                = aws.dr
  description             = "RustLakeDB DR Iceberg Warehouse KMS Key"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_s3_bucket" "primary" {
  bucket = "${var.warehouse_prefix}-${var.primary_region}"
}

resource "aws_s3_bucket" "dr" {
  provider = aws.dr
  bucket   = "${var.warehouse_prefix}-${var.dr_region}-dr"
}

resource "aws_s3_bucket_public_access_block" "primary" {
  bucket                  = aws_s3_bucket.primary.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "dr" {
  provider                = aws.dr
  bucket                  = aws_s3_bucket.dr.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "primary" {
  bucket = aws_s3_bucket.primary.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "dr" {
  provider = aws.dr
  bucket   = aws_s3_bucket.dr.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "primary" {
  bucket = aws_s3_bucket.primary.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.primary.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dr" {
  provider = aws.dr
  bucket   = aws_s3_bucket.dr.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.dr.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "primary" {
  bucket = aws_s3_bucket.primary.id
  rule {
    id     = "expire-noncurrent-iceberg-objects"
    status = "Enabled"
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# IAM Role for S3 Cross-Region Replication (CRR)
resource "aws_iam_role" "crr" {
  name = "${var.warehouse_prefix}-crr-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "crr" {
  name = "${var.warehouse_prefix}-crr-policy"
  role = aws_iam_role.crr.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetReplicationConfiguration", "s3:ListBucket"]
        Resource = [aws_s3_bucket.primary.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObjectVersionForReplication", "s3:GetObjectVersionAcl", "s3:GetObjectVersionTagging"]
        Resource = ["${aws_s3_bucket.primary.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ReplicateObject", "s3:ReplicateDelete", "s3:ReplicateTags"]
        Resource = ["${aws_s3_bucket.dr.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = [aws_kms_key.primary.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Encrypt"]
        Resource = [aws_kms_key.dr.arn]
      }
    ]
  })
}

resource "aws_s3_bucket_replication_configuration" "primary_to_dr" {
  depends_on = [aws_s3_bucket_versioning.primary, aws_s3_bucket_versioning.dr]
  role       = aws_iam_role.crr.arn
  bucket     = aws_s3_bucket.primary.id

  rule {
    id     = "replicate-all-iceberg-data-and-metadata"
    status = "Enabled"
    source_selection_criteria {
      sse_kms_encrypted_objects {
        status = "Enabled"
      }
    }
    destination {
      bucket        = aws_s3_bucket.dr.arn
      storage_class = "STANDARD"
      encryption_configuration {
        replica_kms_key_id = aws_kms_key.dr.arn
      }
    }
  }
}

# IAM Role for RustLakeDB Pods (IRSA) + Iceberg REST STS Vended Credentials
resource "aws_iam_role" "rustlakedb_warehouse_access" {
  name = "${var.warehouse_prefix}-irsa-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = var.eks_oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "rustlakedb_warehouse_access" {
  name = "${var.warehouse_prefix}-irsa-s3-policy"
  role = aws_iam_role.rustlakedb_warehouse_access.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.primary.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${aws_s3_bucket.primary.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.primary.arn]
      }
    ]
  })
}

output "primary_warehouse_uri" {
  value = "s3://${aws_s3_bucket.primary.bucket}"
}

output "dr_warehouse_uri" {
  value = "s3://${aws_s3_bucket.dr.bucket}"
}

output "irsa_role_arn" {
  value = aws_iam_role.rustlakedb_warehouse_access.arn
}
