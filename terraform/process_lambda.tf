resource "aws_lambda_layer_version" "process_layer" {
  layer_name          = "process_layer_pandas"
  compatible_runtimes = ["python3.12"]
  s3_bucket           = aws_s3_object.process_lambda_layer.bucket
  s3_key              = aws_s3_object.process_lambda_layer.key
}

resource "aws_lambda_function" "process_lambda" {
  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.process_lambda_code.key
  function_name    = "process_lambda"
  role             = aws_iam_role.process_lambda_role.arn
  handler          = "process.lambda_handler"
  source_code_hash = filebase64sha256(data.archive_file.process_lambda_deployment_package.output_path)
  timeout          = 60
  runtime          = "python3.12"
  layers           = [aws_lambda_layer_version.process_layer.arn]
  environment {
    variables = {
      S3_INGEST_BUCKET  = aws_s3_bucket.ingest_bucket.bucket,
      S3_PROCESS_BUCKET = aws_s3_bucket.process_bucket.bucket
    }
  }
}
