
data "archive_file" "process_lambda_deployment_package" {
  type        = "zip"
  source_file = "${path.module}/../src/load.py"
  output_path = "${path.module}/../deployment-packages/load_lambda_code.zip"
}

resource "aws_s3_object" "load_lambda_code" {
  bucket      = aws_s3_bucket.lambda_code_bucket.bucket
  key         = "load-lambda/load_lambda_code.zip"
  source      = data.archive_file.load_lambda_deployment_package.output_path
  source_hash = filemd5(data.archive_file.load_lambda_deployment_package.output_path)
}

data "archive_file" "load_lambda_layer" {
  type        = "zip"
  source_dir  = "${path.module}/../deployment-packages/load-layer"
  output_path = "${path.module}/../deployment-packages/load_layer.zip"
}

resource "aws_s3_object" "load_lambda_layer" {
  bucket      = aws_s3_bucket.lambda_code_bucket.bucket
  key         = "load-lambda/load_layer.zip"
  source      = data.archive_file.load_lambda_layer.output_path
  source_hash = filemd5(data.archive_file.load_lambda_layer.output_path)
}