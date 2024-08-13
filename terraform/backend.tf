terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         	   = "vostock-backend-1234ignoreme"
    key                 = "state/terraform.tfstate"
    region         	   = "eu-west-2"
    encrypt        	   = true
  }
}
