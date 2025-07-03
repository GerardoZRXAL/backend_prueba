import os
import shutil
import pytest
import boto3
from moto import mock_aws
from constants import REGION_NAME


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = REGION_NAME


@pytest.fixture
def aws_log_group(aws_credentials):
    with mock_aws():
        yield boto3.client("logs", REGION_NAME)


@pytest.fixture
def aws_secret_manager(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager", region_name=REGION_NAME)


@pytest.fixture
def aws_s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


def pytest_configure(config):
    try:
        shutil.rmtree("mock/output")
    except FileNotFoundError:
        pass
