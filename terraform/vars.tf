variable "bucket_prefix" {
  type = string
  default = "ingest-bucket"
}

variable "iam_role_prefix"{ 
    type = string
    default = "iam_role"
} 

variable "iam_policy_prefix"{ 
    type = string
    default = "iam_policy"
} 


variable "retention_days" {
  description = "The number of days to retain the logs in the CloudWatch log group"
  type        = number
  default     = 30
}
