resource "aws_s3_object" "flink_jar" {
  bucket = var.artifact_bucket_name
  key    = var.flink_jar_key
  source = var.flink_jar_path

  source_hash = filemd5(var.flink_jar_path)

  tags = {
    Environment = var.environment
    Application = var.application_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.application_name}-${var.environment}"
  retention_in_days = 7

  tags = {
    Environment = var.environment
    Application = var.application_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_iam_role" "flink_execution" {
  name = "${var.environment}-flink-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.application_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_role_policy" "flink_execution" {
  name = "${var.environment}-flink-execution-policy"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ReadKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:SubscribeToShard"
        ]

        Resource = var.kinesis_stream_arn
      },
      {
        Sid    = "DecryptKinesisRecords"
        Effect = "Allow"

        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]

        Resource = var.kms_key_arn
      },
      {
        Sid    = "ReadFlinkArtifact"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]

        Resource = "${var.artifact_bucket_arn}/${var.flink_jar_key}"
      },
      {
        Sid    = "ReadArtifactBucketMetadata"
        Effect = "Allow"

        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]

        Resource = var.artifact_bucket_arn
      },
      {
        Sid    = "AccessLakehouseBucket"
        Effect = "Allow"

        Action = [
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads"
        ]

        Resource = var.lakehouse_bucket_arn
      },
      {
        Sid    = "ReadWriteIcebergObjects"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]

        Resource = "${var.lakehouse_bucket_arn}/*"
      },
      {
        Sid    = "AccessGlueCatalog"
        Effect = "Allow"

        Action = [
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ]

        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${var.glue_database_name}/*"
        ]
      },
      {
        Sid    = "WriteCloudWatchLogs"
        Effect = "Allow"

        Action = [
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]

        Resource = [
          aws_cloudwatch_log_group.flink.arn,
          "${aws_cloudwatch_log_group.flink.arn}:*"
        ]
      }
    ]
  })
}

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = "${var.application_name}-${var.environment}"
  description            = "Stateful processing of urban sensor events from Kinesis"
  runtime_environment    = var.runtime_environment
  service_execution_role = aws_iam_role.flink_execution.arn

  application_mode = "STREAMING"

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = var.artifact_bucket_arn
          file_key   = aws_s3_object.flink_jar.key
        }
      }

      code_content_type = "ZIPFILE"
    }

    environment_properties {
      property_group {
        property_group_id = "UrbanSensorsProperties"

        property_map = {
          KINESIS_STREAM_NAME = var.kinesis_stream_name
          AWS_REGION          = var.aws_region
          AWS_ACCOUNT_ID      = var.aws_account_id
          LAKEHOUSE_BUCKET    = var.lakehouse_bucket_name
          ICEBERG_WAREHOUSE   = var.iceberg_warehouse_uri
          GLUE_DATABASE       = var.glue_database_name
          ICEBERG_TABLE       = var.iceberg_table_name
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = var.checkpoint_min_pause_ms
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = var.log_level
        metrics_level      = var.metrics_level
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = var.auto_scaling_enabled
        parallelism          = var.parallelism
        parallelism_per_kpu  = var.parallelism_per_kpu
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = false
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  tags = {
    Environment = var.environment
    Application = var.application_name
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_s3_object.flink_jar,
    aws_iam_role_policy.flink_execution
  ]
}