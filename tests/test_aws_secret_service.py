import base64
import json
import pytest
from constants import REGION_NAME
from secrets_managers.aws_secret_service import (
    AWSSecretService,
    AWSSecretServiceException,
)


def test_get_secret(aws_secret_manager):
    expected_output = {"foo": "bar"}
    data = ""

    with open('resources/unit_tests_constants.json', 'r') as file:
        data = json.load(file)

    aws_secret_manager.create_secret(Name="test", SecretString=data["test_secret"])
    secret_service = AWSSecretService()
    output_generated = secret_service.get_secret("test", REGION_NAME)
    assert output_generated == expected_output


def test_get_secret_value_binary(aws_secret_manager):
    expected_output = "Foo"
    aws_secret_manager.create_secret(
        Name="test",
        SecretBinary=base64.b64encode(expected_output.encode()),
    )
    secret_manager = AWSSecretService()
    output_generated = secret_manager.get_secret("test", REGION_NAME)
    assert output_generated == expected_output


def test_get_secret_json_exception(aws_secret_manager):
    aws_secret_manager.create_secret(Name="test", SecretString="foo")
    secret_manager = AWSSecretService()
    with pytest.raises(AWSSecretServiceException):
        secret_manager.get_secret("test", REGION_NAME)


def test_get_secret_that_does_not_exist(aws_secret_manager):
    secret_manager = AWSSecretService()
    with pytest.raises(AWSSecretServiceException):
        secret_manager.get_secret("Key-Not-Exists", REGION_NAME)
