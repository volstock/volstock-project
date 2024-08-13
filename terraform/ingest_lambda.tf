resource "aws_lambda_function" "ingest_lambda" {
  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.ingest_lambda_code.key
  function_name    = "ingest_lambda"
  role             = aws_iam_role.ingest_lambda_role.arn
  handler          = "extract.lambda_handler"
  source_code_hash = data.archive_file.ingest_lambda_deployment_package.output_base64sha256
  timeout          = 60
  runtime          = "python3.12"
  environment {
    variables = {
      foo = "bar"
    }
  }
}
