variable "environment" {
  description = "Deployment env name"
  type        = string
}

variable "aws_region" {
  description = "AWS region where inf will be deployed"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block assigned to VPC"
  type        = string
}