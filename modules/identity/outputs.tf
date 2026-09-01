output "data_processing_role_arn" {
  value = aws_iam_role.data_processing_role.arn
}

output "audit_role_arn" {
  value = aws_iam_role.audit_role.arn
}