provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

module "network" {
  source      = "../../modules/network"
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
}

module "identity" {
  source = "../../modules/identity"
}

module "kinesis" {
  source             = "../../modules/kinesis"
  environment        = var.environment
  bronze_bucket_name = "bronze-datalake-maxjaida-dev"
}

module "lakehouse" {
  source = "../../modules/lakehouse"

  environment           = var.environment
  aws_account_id        = data.aws_caller_identity.current.account_id
  lakehouse_bucket_name = "lakehouse-maxjaida-dev-${data.aws_caller_identity.current.account_id}"
  glue_database_name    = "lakehouse_db"
  iceberg_table_name    = "urban_sensor_metrics"
  warehouse_prefix      = "warehouse"
  athena_results_prefix = "athena-results"

  tags = {
    Project = "UrbanDataPipeline"
    Module  = "Lakehouse"
  }
}

module "flink" {
  source = "../../modules/flink"

  environment    = var.environment
  aws_region     = "us-east-1"
  aws_account_id = data.aws_caller_identity.current.account_id

  kinesis_stream_name = module.kinesis.stream_name
  kinesis_stream_arn  = module.kinesis.stream_arn
  kms_key_arn         = module.kinesis.kms_key_arn

  artifact_bucket_name = module.kinesis.bronze_bucket_name
  artifact_bucket_arn  = module.kinesis.bronze_bucket_arn

  flink_jar_path = "../../flink-app/target/urban-sensors-flink.jar"
  flink_jar_key  = "flink/urban-sensors-flink-v7-iceberg-1.6.1.jar"

  application_name    = "urban-sensors-flink"
  runtime_environment = "FLINK-1_19"

  checkpoint_interval_ms  = 60000
  checkpoint_min_pause_ms = 30000

  parallelism          = 1
  parallelism_per_kpu  = 1
  auto_scaling_enabled = false

  log_level     = "INFO"
  metrics_level = "TASK"

  lakehouse_bucket_name = module.lakehouse.lakehouse_bucket_name
  lakehouse_bucket_arn  = module.lakehouse.lakehouse_bucket_arn
  iceberg_warehouse_uri = module.lakehouse.warehouse_uri
  glue_database_name    = module.lakehouse.glue_database_name
  iceberg_table_name    = module.lakehouse.iceberg_table_name
}