output "application_name" {
  description = "The name of the Flink application"
  value       = aws_kinesisanalyticsv2_application.flink.name
}

output "application_arn" {
  description = "The ARN of the Flink application"
  value       = aws_kinesisanalyticsv2_application.flink.arn
}

output "application_status" {
  description = "The status of the Flink application"
  value       = aws_kinesisanalyticsv2_application.flink.status
}

output "execution_role_arn" {
  description = "The ARN of the Flink execution role"
  value       = aws_iam_role.flink_execution.arn
}

output "artifact_bucket_name" {
  description = "The name of the S3 bucket for Flink artifacts"
  value       = var.artifact_bucket_name
}

output "artifact_object_key" {
  description = "The key of the Flink artifact in the S3 bucket"
  value       = aws_s3_object.flink_jar.key
}

output "artifact_object_version" {
  description = "The version of the Flink artifact in the S3 bucket"
  value       = aws_s3_object.flink_jar.version_id
}

output "cloudwatch_log_group_name" {
  description = "The name of the CloudWatch log group for Flink"
  value       = aws_cloudwatch_log_group.flink.name
}

output "cloudwatch_log_stream_name" {
  description = "The name of the CloudWatch log stream for Flink"
  value       = aws_cloudwatch_log_stream.flink.name
}
