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

data "archive_file" "process_lambda_layer_pandas" {
  type        = "zip"
  source_dir  = "${path.module}/../deployment-packages/process-layer-pandas"
  output_path = "${path.module}/../deployment-packages/process_layer_pandas.zip"
}

data "archive_file" "process_lambda_layer_pyarrow" {
  type        = "zip"
  source_dir  = "${path.module}/../deployment-packages/process-layer-pyarrow"
  output_path = "${path.module}/../deployment-packages/process_layer_pyarrow.zip"
}

resource "aws_s3_object" "process_lambda_layer_pandas" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "process-lambda/process_layer_pandas.zip"
  source = data.archive_file.process_lambda_layer_pandas.output_path
  source_hash = filemd5(data.archive_file.process_lambda_layer_pandas.output_path)
}

resource "aws_s3_object" "process_lambda_layer_pyarrow" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "process-lambda/process_layer_pyarrow.zip"
  source = data.archive_file.process_lambda_layer_pyarrow.output_path
  source_hash = filemd5(data.archive_file.process_lambda_layer_pyarrow.output_path)
}