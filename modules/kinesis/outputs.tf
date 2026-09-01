output "stream_name" {
  description = "The name of the Kinesis stream"
  value       = aws_kinesis_stream.main.name
}

output "stream_arn" {
  description = "The ARN of the Kinesis stream"
  value       = aws_kinesis_stream.main.arn
}

output "firehose_name" {
  description = "The name of the Kinesis Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.bronze.name
}

output "bronze_bucket_name" {
  description = "The name of the S3 bucket for bronze data"
  value       = aws_s3_bucket.bronze.bucket
}

output "bronze_bucket_arn" {
  description = "The ARN of the S3 bucket for bronze data"
  value       = aws_s3_bucket.bronze.arn
}

output "kms_key_arn" {
  description = "The ARN of the KMS key"
  value       = aws_kms_key.kinesis.arn
}