resource "aws_s3_bucket" "ingest_bucket" {
  bucket_prefix = "ingest-bucket-"
  force_destroy = true
}

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda-deployment-package-"
  force_destroy = true
}

data "archive_file" "ingest_lambda_deployment_package" {
  type        = "zip"
  source_file = "${path.module}/../src/extract.py"
  output_path = "${path.module}/../deployment-packages/ingest_lambda_code.zip"
}

resource "aws_s3_object" "ingest_lambda_code" {
  bucket      = aws_s3_bucket.lambda_code_bucket.bucket
  key         = "ingest-lambda/ingest_lambda_code.zip"
  source      = data.archive_file.ingest_lambda_deployment_package.output_path
  source_hash = filemd5(data.archive_file.ingest_lambda_deployment_package.output_path)
}

data "archive_file" "ingest_lambda_layer" {
  type        = "zip"
  source_dir  = "${path.module}/../deployment-packages/layer-ingest"
  output_path = "${path.module}/../deployment-packages/ingest_layer.zip"
}

resource "aws_s3_object" "ingest_lambda_layer" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "ingest-lambda/ingest_layer.zip"
  source = data.archive_file.ingest_lambda_layer.output_path
  source_hash = filemd5(data.archive_file.ingest_lambda_layer.output_path)
}
