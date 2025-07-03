import os
from testfixtures import LogCapture
from loggers.aws_log_service import AWSLogService
from constants import LOG_APP_NAME
import boto3
from moto import mock_aws


@mock_aws
def test_log_error(aws_log_group):
    log_group_name = "generic-log-group"
    boto3.setup_default_session(region_name="us-east-1")
    logs = boto3.client("logs")
    logs.create_log_group(logGroupName=log_group_name)

    input_message = "There was an error"
    log_utils = AWSLogService()
    with LogCapture() as logs:
        log_utils.log_error(input_message)


@mock_aws
def test_log_info(aws_log_group):
    log_group_name = "generic-log-group"
    boto3.setup_default_session(region_name="us-east-1")
    logs = boto3.client("logs")
    logs.create_log_group(logGroupName=log_group_name)

    key = "TEST"
    value = "hello world"
    expected_output = "{'key': 'TEST', 'value': 'hello world', 'type': 'INFO'}"
    log_utils = AWSLogService()
    with LogCapture() as logs:
        log_utils.log_info({"key": key, "value": value})
