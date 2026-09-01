output "lakehouse_bucket_name" {
  description = "The name of the S3 bucket used for the lakehouse."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_bucket_arn" {
  description = "The ARN of the S3 bucket used for the lakehouse."
  value       = aws_s3_bucket.lakehouse.arn
}

output "warehouse_uri" {
  description = "The S3 URI for the Athena query results location."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/${trim(var.athena_results_prefix, "/")}/"
}

output "glue_database_name" {
  description = "The name of the AWS Glue database for the lakehouse."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "glue_database_arn" {
  description = "The ARN of the AWS Glue database for the lakehouse."
  value       = aws_glue_catalog_database.lakehouse.arn
}

output "iceberg_table_name" {
  description = "The name of the Iceberg table for the lakehouse."
  value       = var.iceberg_table_name
}

output "iceberg_table_location" {
  description = "The S3 location of the Iceberg table for the lakehouse."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/${trim(var.warehouse_prefix, "/")}/${var.glue_database_name}/${var.iceberg_table_name}/"
}

output "athena_workgroup_name" {
  description = "The name of the Athena workgroup for the lakehouse."
  value       = aws_athena_workgroup.lakehouse.name
}

output "athena_results_location" {
  description = "The S3 location for Athena query results."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/${trim(var.athena_results_prefix, "/")}/"
}