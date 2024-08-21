resource "aws_iam_role" "process_lambda_role" {
  name               = "process_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.trust_policy_lambdas.json
}


data "aws_iam_policy_document" "cw_process_document" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:*"]
  }
  statement {
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["arn:aws:logs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:log-group:/aws/lambda/process_lambda:*"]
  }
}

resource "aws_iam_policy" "cw_policy_process" {
  name   = "cw-policy-process-lambda"
  policy = data.aws_iam_policy_document.cw_process_document.json
}

resource "aws_iam_role_policy_attachment" "cw_process_policy_attachemnt" {
  role       = aws_iam_role.process_lambda_role.name
  policy_arn = aws_iam_policy.cw_policy_process.arn
}


data "aws_iam_policy_document" "s3_process_document" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.process_bucket.arn}/*"]
  }
  statement {
    actions = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.ingest_bucket.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["${aws_s3_bucket.process_bucket.arn}", "${aws_s3_bucket.ingest_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "s3_policy_process" {
  name   = "s3-policy-process-lambda"
  policy = data.aws_iam_policy_document.s3_process_document.json
}

resource "aws_iam_role_policy_attachment" "s3_process_policy_attachment" {
  role       = aws_iam_role.process_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy_process.arn
}
