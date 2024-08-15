resource "aws_sns_topic" "ingest_lambda_critical_error" {
  name = "ingest-lambda-critical-error"
}

resource "aws_sns_topic_subscription" "email_ingest_lambda_critical_error" {
  topic_arn = aws_sns_topic.ingest_lambda_critical_error.arn
  protocol  = "email"
  endpoint  = "tudorsonycx@gmail.com"
}

resource "aws_cloudwatch_log_metric_filter" "metric_filter_ingest" {
  name           = "IngestCriticalError"
  pattern        = "CRITICAL"
  log_group_name = "/aws/lambda/ingest_lambda"
  
  metric_transformation {
    name      = "IngestCriticalErrorCount"
    namespace = "IngestLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "logging_error_alarm" {
  alarm_name          = "${aws_sns_topic.ingest_lambda_critical_error.name}-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = aws_cloudwatch_log_metric_filter.metric_filter_ingest.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.metric_filter_ingest.metric_transformation[0].namespace
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "CRITICAL error encountered"
  alarm_actions       = [aws_sns_topic.ingest_lambda_critical_error.arn]
}
