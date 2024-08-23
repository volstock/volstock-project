resource "aws_lambda_layer_version" "load_layer" {
  layer_name          = "load_layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket           = aws_s3_object.load_lambda_layer.bucket
  s3_key              = aws_s3_object.load_lambda_layer.key
}

resource "aws_lambda_function" "load_lambda" {
  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.load_lambda_code.key
  function_name    = "load_lambda"
  role             = aws_iam_role.load_lambda_role.arn
  handler          = "load.lambda_handler"
  source_code_hash = filebase64sha256(data.archive_file.load_lambda_deployment_package.output_path)
  timeout          = 900
  runtime          = "python3.12"
  layers           = [aws_lambda_layer_version.load_layer.arn]
  environment {
    variables = {
      S3_PROCESS_BUCKET = aws_s3_bucket.process_bucket.bucket
    }
  }
}
