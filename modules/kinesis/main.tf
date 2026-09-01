data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name

  tags = {
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_iam_role" "firehose_role" {
  name = "${var.environment}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"

        Principal = {
          Service = "firehose.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_kms_key" "kinesis" {
  description             = "KMS key for Kinesis"
  deletion_window_in_days = 7

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      {
        Sid    = "EnableRootPermissions"
        Effect = "Allow"

        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }

        Action   = "kms:*"
        Resource = "*"
      },

      {
        Sid    = "AllowFirehoseRole"
        Effect = "Allow"

        Principal = {
          AWS = aws_iam_role.firehose_role.arn
        }

        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]

        Resource = "*"
      }
    ]
  })
}

resource "aws_kinesis_stream" "main" {
  name             = "${var.environment}-stream"
  shard_count      = 2
  retention_period = 24

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.kinesis.arn

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "${var.environment}-firehose-policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      {
        Effect = "Allow"

        Action = [
          "kinesis:Get*",
          "kinesis:DescribeStream",
          "kinesis:ListShards"
        ]

        Resource = aws_kinesis_stream.main.arn
      },

      {
        Effect = "Allow"

        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]

        Resource = aws_kms_key.kinesis.arn
      },

      {
        Effect = "Allow"

        Action = [
          "s3:PutObject",
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:AbortMultipartUpload",
          "s3:GetObject",
          "s3:ListBucketMultipartUploads"
        ]

        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*"
        ]
      },

      {
        Effect = "Allow"

        Action = [
          "logs:PutLogEvents",
          "logs:CreateLogStream",
          "logs:CreateLogGroup"
        ]

        Resource = "*"
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "firehose" {
  name = "/aws/kinesisfirehose/${var.environment}-firehose"
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_kinesis_firehose_delivery_stream" "bronze" {
  name        = "${var.environment}-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.main.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.bronze.arn

    buffering_size     = 5
    buffering_interval = 60

    prefix = "ingesta/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    error_output_prefix = "errores/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "write_alarm" {
  alarm_name          = "${var.environment}-write-throughput-exceeded"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1

  metric_name = "WriteProvisionedThroughputExceeded"
  namespace   = "AWS/Kinesis"

  period    = 60
  statistic = "Sum"
  threshold = 0

  dimensions = {
    StreamName = aws_kinesis_stream.main.name
  }
}

resource "aws_cloudwatch_metric_alarm" "read_alarm" {
  alarm_name          = "${var.environment}-read-throughput-exceeded"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1

  metric_name = "ReadProvisionedThroughputExceeded"
  namespace   = "AWS/Kinesis"

  period    = 60
  statistic = "Sum"
  threshold = 0

  dimensions = {
    StreamName = aws_kinesis_stream.main.name
  }
}