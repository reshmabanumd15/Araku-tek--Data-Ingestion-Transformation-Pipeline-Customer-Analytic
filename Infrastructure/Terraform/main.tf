terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
  }
}
provider "aws" { region = var.region }

variable "region" { type = string }
variable "bucket" { type = string }
variable "redshift_role_arn" { type = string }

# TODO: Add S3, IAM, Glue, Crawler, Lambda resources
