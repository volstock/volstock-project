resource "aws_s3_bucket" "ingest_bucket" {
  bucket_prefix = "${var.bucket_prefix}-"
}

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = "lambda_deployment_package-"
}

resource "aws_s3_object" "ingest_lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "ingest-lambda/ingest_lambda_code.zip"
  source = data.archive_file.ingest_lambda_deployment_package.output_path
}

data "archive_file" "ingest_lambda_deployment_package" {
  type        = "zip"
  source_file = "${path.module}/../src/extract.py"
  output_path = "${path.module}/../deployment-packages/ingest_lambda_code.zip"
}
