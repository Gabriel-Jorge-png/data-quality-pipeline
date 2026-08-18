terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  required_version = ">= 1.5.0"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "data_quality" {
  bucket = "data-quality-pipeline-gabriel"

  tags = {
    Name        = "data-quality-pipeline"
    Environment = "dev"
    Project     = "data-quality-pipeline"
  }
}

resource "aws_s3_bucket_public_access_block" "data_quality" {
  bucket = aws_s3_bucket.data_quality.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "raw" {
  bucket = aws_s3_bucket.data_quality.id
  key    = "raw/"
}

resource "aws_s3_object" "logs" {
  bucket = aws_s3_bucket.data_quality.id
  key    = "logs/"
}

resource "aws_iam_role" "glue_role" {
  name = "data-quality-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "data-quality-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.data_quality.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.data_quality.arn}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_s3_object" "scripts" {
  bucket = aws_s3_bucket.data_quality.id
  key    = "scripts/"
}

resource "aws_glue_job" "data_quality_job" {
  name     = "data-quality-pipeline-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_quality.bucket}/scripts/glue_job.py"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  default_arguments = {
    "--logs-path"      = "s3://${aws_s3_bucket.data_quality.bucket}/logs/"
    "--extra-py-files" = "s3://${aws_s3_bucket.data_quality.bucket}/scripts/glue_dependencies.zip"

    "--vendas-path"   = "s3://${aws_s3_bucket.data_quality.bucket}/raw/vendas/vendas.csv"
    "--clientes-path" = "s3://${aws_s3_bucket.data_quality.bucket}/raw/clientes/clientes.csv"

    "--trusted-path"    = "s3://${aws_s3_bucket.data_quality.bucket}/trusted/"
    "--quarantine-path" = "s3://${aws_s3_bucket.data_quality.bucket}/quarantine/"
  }
}

resource "aws_glue_catalog_database" "data_quality" {
  name        = "data_quality_db"
  description = "Data Catalog do pipeline de qualidade de dados"
}

resource "aws_glue_crawler" "trusted_crawler" {
  name          = "data-quality-trusted-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.data_quality.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_quality.bucket}/trusted/"
  }
}

resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = aws_s3_bucket.data_quality.id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "new_sales_file" {
  name        = "data-quality-new-sales-file"
  description = "Dispara quando um arquivo e criado em raw/vendas"

  event_pattern = jsonencode({
    source = ["aws.s3"]

    detail-type = ["Object Created"]

    detail = {
      bucket = {
        name = [aws_s3_bucket.data_quality.bucket]
      }

      object = {
        key = [{
          prefix = "raw/vendas/"
        }]
      }
    }
  })
}

resource "aws_iam_role" "eventbridge_role" {
  name = "data-quality-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_glue_policy" {
  name = "data-quality-eventbridge-glue-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:NotifyEvent"
        ]
        Resource = aws_glue_workflow.data_quality_workflow.arn
      }
    ]
  })
}

resource "aws_glue_workflow" "data_quality_workflow" {
  name        = "data-quality-workflow"
  description = "Workflow do pipeline de qualidade de dados"
}

resource "aws_glue_trigger" "data_quality_event_trigger" {
  name          = "data-quality-event-trigger"
  type          = "EVENT"
  workflow_name = aws_glue_workflow.data_quality_workflow.name

  actions {
    job_name = aws_glue_job.data_quality_job.name
  }
}

resource "aws_cloudwatch_event_target" "glue_workflow" {
  rule      = aws_cloudwatch_event_rule.new_sales_file.name
  target_id = "data-quality-glue-workflow"
  arn       = aws_glue_workflow.data_quality_workflow.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}