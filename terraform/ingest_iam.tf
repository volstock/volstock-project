data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ingest_lambda_role" {
  name               = "ingest_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

data "aws_caller_identity" "current_account" {}

data "aws_region" "current_region" {}

data "aws_iam_policy_document" "cw_ingest_document" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:*"]
  }
  statement {
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["arn:aws:logs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:log-group:/aws/lambda/ingest_lambda:*"]
  }
}

resource "aws_iam_policy" "cw_policy_ingest" {
  name   = "cw-policy-ingest-lambda"
  policy = data.aws_iam_policy_document.cw_ingest_document.json
}

resource "aws_iam_role_policy_attachment" "cw_ingest_policy_attachemnt" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy_ingest.arn
}


data "aws_iam_policy_document" "s3_ingest_document" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.ingest_bucket.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["${aws_s3_bucket.ingest_bucket.arn}"]
  }
}

resource "aws_iam_policy" "s3_policy_ingest" {
  name   = "s3-policy-ingest-lambda"
  policy = data.aws_iam_policy_document.s3_ingest_document.json
}

resource "aws_iam_role_policy_attachment" "s3_ingest_policy_attachment" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy_ingest.arn
}

data "aws_secretsmanager_secret" "totesys_db_name" {
  name = "db_name"
}

data "aws_secretsmanager_secret" "totesys_db_host" {
  name = "db_host"
}

data "aws_secretsmanager_secret" "totesys_db_user" {
  name = "db_user"
}

data "aws_secretsmanager_secret" "totesys_db_pass" {
  name = "db_pass"
}

data "aws_iam_policy_document" "sm_ingest_document" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      "${data.aws_secretsmanager_secret.totesys_db_name.arn}",
      "${data.aws_secretsmanager_secret.totesys_db_host.arn}",
      "${data.aws_secretsmanager_secret.totesys_db_user.arn}",
      "${data.aws_secretsmanager_secret.totesys_db_pass.arn}"
    ]
  }
}

resource "aws_iam_policy" "sm_policy_ingest" {
  name   = "sm-policy-ingest-lambda"
  policy = data.aws_iam_policy_document.sm_ingest_document.json
}

resource "aws_iam_role_policy_attachment" "sm_ingest_policy_attachment" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = aws_iam_policy.sm_policy_ingest.arn
}
