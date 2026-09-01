terraform {
  backend "s3" {
    bucket         = "terraform-state-bucket-maxjaida-dev"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}