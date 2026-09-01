variable "environment" {
  description = "The environment for the Flink application"
  type        = string
}

variable "aws_region" {
  description = "The AWS region for the Flink application"
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "The AWS account ID for the Flink application"
  type        = string
}

variable "kinesis_stream_name" {
  description = "The name of the Kinesis input stream"
  type        = string
}

variable "kinesis_stream_arn" {
  description = "The ARN of the Kinesis input stream"
  type        = string
}

variable "kms_key_arn" {
  description = "The ARN of the KMS key used by the Kinesis stream"
  type        = string
}

variable "artifact_bucket_name" {
  description = "The name of the S3 bucket containing the Flink artifact"
  type        = string
}

variable "artifact_bucket_arn" {
  description = "The ARN of the S3 bucket containing the Flink artifact"
  type        = string
}

variable "flink_jar_path" {
  description = "The local path to the Flink application JAR"
  type        = string
}

variable "flink_jar_key" {
  description = "The S3 object key for the Flink application JAR"
  type        = string
  default     = "flink/urban-sensors-flink.jar"
}

variable "application_name" {
  description = "The name of the Managed Service for Apache Flink application"
  type        = string
  default     = "urban-sensors-flink"
}

variable "runtime_environment" {
  description = "The Apache Flink runtime environment"
  type        = string
  default     = "FLINK-1_19"
}

variable "checkpoint_interval_ms" {
  description = "The checkpoint interval in milliseconds"
  type        = number
  default     = 60000

  validation {
    condition     = var.checkpoint_interval_ms >= 1000
    error_message = "Checkpointing interval must be at least 1000 milliseconds."
  }
}

variable "checkpoint_min_pause_ms" {
  description = "The minimum pause between checkpoints in milliseconds"
  type        = number
  default     = 30000

  validation {
    condition     = var.checkpoint_min_pause_ms >= 0
    error_message = "Minimum pause between checkpoints cannot be negative."
  }
}

variable "parallelism" {
  description = "The initial parallelism for the Flink application"
  type        = number
  default     = 1

  validation {
    condition     = var.parallelism >= 1
    error_message = "Parallelism must be greater than or equal to 1."
  }
}

variable "parallelism_per_kpu" {
  description = "The number of parallel tasks per KPU"
  type        = number
  default     = 1

  validation {
    condition     = var.parallelism_per_kpu >= 1
    error_message = "Parallelism per KPU must be greater than or equal to 1."
  }
}

variable "auto_scaling_enabled" {
  description = "Whether automatic scaling is enabled"
  type        = bool
  default     = false
}

variable "log_level" {
  description = "The log level for the Flink application"
  type        = string
  default     = "INFO"

  validation {
    condition = contains(
      ["DEBUG", "INFO", "WARN", "ERROR"],
      var.log_level
    )

    error_message = "Log level must be DEBUG, INFO, WARN, or ERROR."
  }
}

variable "metrics_level" {
  description = "The metrics level for the Flink application"
  type        = string
  default     = "TASK"

  validation {
    condition = contains(
      ["APPLICATION", "TASK", "OPERATOR", "PARALLELISM"],
      var.metrics_level
    )

    error_message = "Metrics level must be APPLICATION, TASK, OPERATOR, or PARALLELISM."
  }
}

variable "lakehouse_bucket_name" {
  description = "The name of the S3 bucket used by the Iceberg Lakehouse"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "The ARN of the S3 bucket used by the Iceberg Lakehouse"
  type        = string
}

variable "iceberg_warehouse_uri" {
  description = "The S3 warehouse URI used by Apache Iceberg"
  type        = string
}

variable "glue_database_name" {
  description = "The name of the AWS Glue Data Catalog database"
  type        = string
  default     = "lakehouse_db"
}

variable "iceberg_table_name" {
  description = "The name of the Apache Iceberg table"
  type        = string
  default     = "urban_sensor_metrics"
}