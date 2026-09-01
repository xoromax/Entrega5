variable "environment" {
  description = "The environment for the lakehouse deployment (e.g., dev, staging, prod)."
  type        = string
}

variable "aws_account_id" {
  description = "The AWS account ID where the lakehouse resources will be deployed."
  type        = string
}

variable "lakehouse_bucket_name" {
  description = "The name of the S3 bucket to be used for the lakehouse."
  type        = string
}

variable "glue_database_name" {
  description = "The name of the AWS Glue database for the lakehouse."
  type        = string
  default     = "lakehouse_db"

  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.glue_database_name))
    error_message = "The Glue database name must match the pattern [a-z0-9_]."
  }
}

variable "iceberg_table_name" {
  description = "The name of the Iceberg table for the lakehouse."
  type        = string
  default     = "lakehouse_table"

  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.iceberg_table_name))
    error_message = "The Iceberg table name must match the pattern [a-z0-9_]."
  }
}

variable "warehouse_prefix" {
  description = "The prefix for the warehouse path in S3."
  type        = string
  default     = "warehouse"
}

variable "athena_results_prefix" {
  description = "The prefix for the Athena query results path in S3."
  type        = string
  default     = "athena-results"
}

variable "tags" {
  description = "A map of tags to apply to all resources in the lakehouse."
  type        = map(string)
  default     = {}
}

