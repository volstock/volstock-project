terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         	   = "volstock-test-backend-13082024"
    key                 = "state/terraform.tfstate"
    region         	   = "eu-west-2"
    encrypt        	   = true
  }
}