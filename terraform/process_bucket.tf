resource "aws_s3_bucket" "process_bucket" {
  bucket_prefix = "process-bucket-"
  force_destroy = true
}


data "archive_file" "process_lambda_deployment_package" {
  type        = "zip"
  source_file = "${path.module}/../src/process.py"
  output_path = "${path.module}/../deployment-packages/process_lambda_code.zip"
}

resource "aws_s3_object" "process_lambda_code" {
  bucket      = aws_s3_bucket.lambda_code_bucket.bucket
  key         = "process-lambda/process_lambda_code.zip"
  source      = data.archive_file.process_lambda_deployment_package.output_path
  source_hash = filemd5(data.archive_file.process_lambda_deployment_package.output_path)
}

data "archive_file" "process_lambda_layer" {
  type        = "zip"
  source_dir  = "${path.module}/../deployment-packages/process-layer"
  output_path = "${path.module}/../deployment-packages/process_layer.zip"
}

resource "aws_s3_object" "process_lambda_layer" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "process-lambda/process_layer.zip"
  source = data.archive_file.process_lambda_layer.output_path
  source_hash = filemd5(data.archive_file.process_lambda_layer.output_path)
}