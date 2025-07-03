import pytest
import boto3
from aws_utils.sns_utils import SnsUtils, SnsUtilsException
from moto import mock_aws


@mock_aws
def test_publish_to_sns():
    conn = boto3.client("sns", region_name="us-east-1")
    mock_topic = conn.create_topic(Name="mocktopic")
    response = conn.list_topics()
    print(response)
    SnsUtils.publish_to_sns(
        response.get("Topics")[0].get("TopicArn"), "Message title", "Message content"
    )

@mock_aws
def test_publish_to_sns_error():
    with pytest.raises(SnsUtilsException):
        SnsUtils.publish_to_sns(
            "test_error", "Message title", "Message content"
        )
