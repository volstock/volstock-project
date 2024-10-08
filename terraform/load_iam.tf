resource "aws_iam_role" "load_lambda_role" {
  name               = "load_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.trust_policy_lambdas.json
}


data "aws_iam_policy_document" "cw_load_document" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:*"]
  }
  statement {
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["arn:aws:logs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:log-group:/aws/lambda/load_lambda:*"]
  }
}

resource "aws_iam_policy" "cw_policy_load" {
  name   = "cw-policy-load-lambda"
  policy = data.aws_iam_policy_document.cw_load_document.json
}

resource "aws_iam_role_policy_attachment" "cw_load_policy_attachemnt" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy_load.arn
}


data "aws_iam_policy_document" "s3_load_document" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.process_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "s3_policy_load" {
  name   = "s3-policy-load-lambda"
  policy = data.aws_iam_policy_document.s3_load_document.json
}

resource "aws_iam_role_policy_attachment" "s3_load_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy_load.arn
}

data "aws_secretsmanager_secret" "warehouse_db_name" {
  name = "whdb_name"
}

data "aws_secretsmanager_secret" "warehouse_db_host" {
  name = "whdb_host"
}

data "aws_secretsmanager_secret" "warehouse_db_user" {
  name = "whdb_user"
}

data "aws_secretsmanager_secret" "warehouse_db_pass" {
  name = "whdb_pass"
}

data "aws_iam_policy_document" "sm_load_document" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      "${data.aws_secretsmanager_secret.warehouse_db_name.arn}",
      "${data.aws_secretsmanager_secret.warehouse_db_host.arn}",
      "${data.aws_secretsmanager_secret.warehouse_db_user.arn}",
      "${data.aws_secretsmanager_secret.warehouse_db_pass.arn}",
    ]
  }
}

resource "aws_iam_policy" "sm_policy_load" {
  name   = "sm-policy-load-lambda"
  policy = data.aws_iam_policy_document.sm_load_document.json
}

resource "aws_iam_role_policy_attachment" "sm_load_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.sm_policy_load.arn
}
