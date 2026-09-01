resource "aws_s3_bucket" "lakehouse" {
  bucket = var.lakehouse_bucket_name

  tags = merge(
    var.tags,
    {
      Name        = var.lakehouse_bucket_name
      Environment = var.environment
      Layer       = "lakehouse"
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "warehouse_directory" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "${trim(var.warehouse_prefix, "/")}/"
  content = ""

  depends_on = [
    aws_s3_bucket_versioning.lakehouse
  ]
}

resource "aws_s3_object" "athena_results_directory" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "${trim(var.athena_results_prefix, "/")}/"
  content = ""

  depends_on = [
    aws_s3_bucket_versioning.lakehouse
  ]
}

resource "aws_glue_catalog_database" "lakehouse" {
  catalog_id  = var.aws_account_id
  name        = var.glue_database_name
  description = "Glue Data Catalog database for urban sensor Iceberg tables"

  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/${trim(var.warehouse_prefix, "/")}/"

  parameters = {
    environment  = var.environment
    table_format = "ICEBERG"
  }

  tags = merge(
    var.tags,
    {
      Name        = var.glue_database_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )

  depends_on = [
    aws_s3_bucket_versioning.lakehouse
  ]
}

resource "aws_athena_workgroup" "lakehouse" {
  name        = "${var.environment}-lakehouse-workgroup"
  description = "Athena workgroup for querying the urban sensors Iceberg table"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${trim(var.athena_results_prefix, "/")}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = merge(
    var.tags,
    {
      Name        = "${var.environment}-lakehouse-workgroup"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )

  depends_on = [
    aws_s3_object.athena_results_directory
  ]
}