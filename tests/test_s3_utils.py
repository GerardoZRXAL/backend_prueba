import pytest
import boto3
from moto import mock_aws
from aws_utils.s3_utils import s3Utils, s3UtilsException


@mock_aws
def test_download_file_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket="test_bucket")
    file_path = "mock/config_files/test.sql"
    bucket = "test_bucket"
    object_key = "test/test.sql"
    s3_client.upload_file(file_path, bucket, object_key)
    s3Utils.download_file_s3("mock/upload/test.sql", bucket, object_key)

def test_download_file_s3_error():
    object_key = "test/test.sql"
    bucket = "test_bucket"
    with pytest.raises(s3UtilsException):
        s3Utils.download_file_s3("mock/upload/test.sql", bucket, object_key)
