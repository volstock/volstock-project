resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "ingest-process-load-state-machine"
  role_arn = aws_iam_role.state_machine_role.arn

  definition = <<EOF
{
  "Comment": "A description of my state machine",
  "StartAt": "Ingest data",
  "States": {
    "Ingest data": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:function:${aws_lambda_function.ingest_lambda.function_name}:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "Process ingested data"
    },
    "Process ingested data": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "arn:aws:lambda:${data.aws_region.current_region.name}:${data.aws_caller_identity.current_account.account_id}:function:${aws_lambda_function.process_lambda.function_name}:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "End": true
    }
  }
}
EOF
}

data "aws_iam_policy_document" "trust_policy_states" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "state_machine_role" {
  name               = "state_machine_role_for_lambdas"
  assume_role_policy = data.aws_iam_policy_document.trust_policy_states.json
}

data "aws_iam_policy_document" "lambdas_state_machine_document" {
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = ["${aws_lambda_function.ingest_lambda.arn}:*", "${aws_lambda_function.process_lambda.arn}:*"]
  }
}

resource "aws_iam_policy" "lambdas_state_machine_policy" {
  name   = "lambdas_access_state_machine_policy"
  policy = data.aws_iam_policy_document.lambdas_state_machine_document.json
}

resource "aws_iam_role_policy_attachment" "lambdas_state_machine_policy_attachment" {
  role       = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.lambdas_state_machine_policy.arn
}

data "aws_iam_policy_document" "trust_policy_events" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "eventbridge_role" {
  name               = "event_bridge_role_for_state_machine_execution"
  assume_role_policy = data.aws_iam_policy_document.trust_policy_events.json
}

data "aws_iam_policy_document" "eventbridge_states_document" {
  statement {
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = ["${aws_sfn_state_machine.sfn_state_machine.arn}"]
  }
}

resource "aws_iam_policy" "eventbridge_states_policy" {
  name   = "eventbridge_states_execute_policy"
  policy = data.aws_iam_policy_document.eventbridge_states_document.json
}

resource "aws_iam_role_policy_attachment" "eventbridge_states_policy_attachment" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_states_policy.arn
}

resource "aws_cloudwatch_event_rule" "state_machine_scheduler" {
  schedule_expression = "rate(2 minutes)"
}

resource "aws_cloudwatch_event_target" "state_machine_every_30_min" {
  rule     = aws_cloudwatch_event_rule.state_machine_scheduler.name
  arn      = aws_sfn_state_machine.sfn_state_machine.arn
  role_arn = aws_iam_role.eventbridge_role.arn
}
