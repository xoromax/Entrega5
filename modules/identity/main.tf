data "aws_caller_identity" "current" {}

resource "aws_iam_role" "data_processing_role" {
  name = "data_processing_role"

  assume_role_policy = jsonencode({
    "Version" = "2012-10-17",
    "Statement" = [
      {
        "Effect" = "Allow",
        "Principal" = {
          "Service" = [
            "lambda.amazonaws.com",
            "kinesisanalytics.amazonaws.com"
          ]
        },
        "Action" = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "data_processing_policy" {
  name = "data_processing_policy"

  policy = jsonencode({
    "Version" = "2012-10-17",
    "Statement" = [
      {
        "Effect" = "Allow",
        "Action" = [
          "s3:ListBucket"
        ],
        "Resource" = [
          "arn:aws:s3:::data-lake-bucket"
        ]
      },
      {
        "Effect" = "Allow",
        "Action" = [
          "s3:GetObject",
          "s3:PutObject"
        ],
        "Resource" = [
          "arn:aws:s3:::data-lake-bucket/raw/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "processing_attach" {
  role       = aws_iam_role.data_processing_role.name
  policy_arn = aws_iam_policy.data_processing_policy.arn
}

resource "aws_iam_role" "audit_role" {
  name = "audit_readonly_role"

  assume_role_policy = jsonencode({
    "Version" = "2012-10-17",
    "Statement" = [{
      "Effect" = "Allow",
      "Principal" = {
        AWS = data.aws_caller_identity.current.account_id
      }
      "Action" = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "readonly_attach" {
  role       = aws_iam_role.audit_role.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}