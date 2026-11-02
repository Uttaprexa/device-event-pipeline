# SQS queue + dead-letter queue + redrive policy + alarm. NOT validated in the sandbox this
# was written in (no terraform binary): run `terraform init && terraform validate && terraform plan`.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "region" { default = "us-west-1" }
variable "max_receive_count" { default = 5 }
variable "visibility_timeout_seconds" { default = 30 }

provider "aws" { region = var.region }

resource "aws_sqs_queue" "dlq" {
  name                      = "device-events-dlq"
  message_retention_seconds = 1209600 # 14 days to inspect/redrive
}

resource "aws_sqs_queue" "events" {
  name                       = "device-events"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.events.arn]
  })
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "device-events-dlq-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

output "queue_url" { value = aws_sqs_queue.events.url }
output "dlq_url" { value = aws_sqs_queue.dlq.url }
