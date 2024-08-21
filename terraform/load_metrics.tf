resource "aws_cloudwatch_log_group" "load_lambda_log_group" {
  name = "/aws/lambda/${aws_lambda_function.load_lambda.function_name}"

}
resource "aws_sns_topic" "load_lambda_critical_error" {
  name = "load-lambda-critical-error"
}

resource "aws_sns_topic_subscription" "email_load_lambda_critical_error" {
  topic_arn = aws_sns_topic.load_lambda_critical_error.arn
  protocol  = "email"
  endpoint  = "tudorsonycx@gmail.com"
}

resource "aws_cloudwatch_log_metric_filter" "metric_filter_load" {
  name           = "LoadCriticalError"
  pattern        = "CRITICAL"
  log_group_name = aws_cloudwatch_log_group.load_lambda_log_group.name
  
  metric_transformation {
    name      = "LoadCriticalErrorCount"
    namespace = "LoadLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "logging_error_alarm_load" {
  alarm_name          = "${aws_sns_topic.load_lambda_critical_error.name}-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = aws_cloudwatch_log_metric_filter.metric_filter_load.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.metric_filter_load.metric_transformation[0].namespace
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "CRITICAL error encountered"
  alarm_actions       = [aws_sns_topic.load_lambda_critical_error.arn]
}