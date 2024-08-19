resource "aws_cloudwatch_log_group" "process_lambda_log_group" {
  name = "/aws/lambda/${aws_lambda_function.process_lambda.function_name}"

}
resource "aws_sns_topic" "process_lambda_critical_error" {
  name = "process-lambda-critical-error"
}

resource "aws_sns_topic_subscription" "email_process_lambda_critical_error" {
  topic_arn = aws_sns_topic.process_lambda_critical_error.arn
  protocol  = "email"
  endpoint  = "tudorsonycx@gmail.com"
}

resource "aws_cloudwatch_log_metric_filter" "metric_filter_process" {
  name           = "ProcessCriticalError"
  pattern        = "CRITICAL"
  log_group_name = aws_cloudwatch_log_group.process_lambda_log_group.name
  
  metric_transformation {
    name      = "ProcessCriticalErrorCount"
    namespace = "ProcessLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "logging_error_alarm_process" {
  alarm_name          = "${aws_sns_topic.process_lambda_critical_error.name}-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = aws_cloudwatch_log_metric_filter.metric_filter_process.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.metric_filter_process.metric_transformation[0].namespace
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "CRITICAL error encountered"
  alarm_actions       = [aws_sns_topic.process_lambda_critical_error.arn]
}
