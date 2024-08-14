resource "aws_lambda_layer_version" "ingest_layer" {
  layer_name          = "ingest_layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket           = aws_s3_object.ingest_lambda_layer.bucket
  s3_key              = aws_s3_object.ingest_lambda_layer.key
}

resource "aws_lambda_function" "ingest_lambda" {
  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.ingest_lambda_code.key
  function_name    = "ingest_lambda"
  role             = aws_iam_role.ingest_lambda_role.arn
  handler          = "extract.lambda_handler"
  source_code_hash = filebase64sha256(data.archive_file.ingest_lambda_deployment_package.output_path)
  timeout          = 60
  runtime          = "python3.12"
  layers           = [aws_lambda_layer_version.ingest_layer.arn]
  environment {
    variables = {
      S3_INGEST_BUCKET = aws_s3_bucket.ingest_bucket.bucket
    }
  }
}
