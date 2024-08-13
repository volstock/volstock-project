resource "aws_s3_bucket" "ingest_bucket" {
    
  tags = {
    bucket = "ingestion-bucket-13082024"
    Name        = "ingest-bucket"
    Environment = "Dev"
  }
}