output "vpc_id" {
  value = module.network.vpc_id
}

output "private_subnet_ids" {
  value = module.network.private_subnet_ids
}

output "data_processing_role_arn" {
  value = module.identity.data_processing_role_arn
}

output "audit_role_arn" {
  value = module.identity.audit_role_arn
}

output "stream_name" {
  value = module.kinesis.stream_name
}

output "stream_arn" {
  value = module.kinesis.stream_arn
}

output "firehose_name" {
  value = module.kinesis.firehose_name
}

output "bronze_bucket_name" {
  value = module.kinesis.bronze_bucket_name
}