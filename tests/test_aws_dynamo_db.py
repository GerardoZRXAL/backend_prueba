import pytest
import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_aws
from document_db_manager.aws_dynamo_db import AWSDynamoDb, AWSDynamoDbException


@mock_aws
def test_get_data_from_table():
    dynamo_tbl_name = "datalake-test"
    conn = boto3.resource("dynamodb", region_name="us-east-1")
    table_response = conn.create_table(
        TableName=dynamo_tbl_name,
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # Insert test data into the table
    table = conn.Table(dynamo_tbl_name)
    table.put_item(Item={"key": "test_key", "value": "test_value"})

    AWSDynamoDb.get_data_from_table(dynamo_tbl_name, "key", "test_key", "us-east-1")


@mock_aws
def test_get_data_from_table_exception():
    dynamo_tbl_name = "datalake-test"
    with pytest.raises(AWSDynamoDbException):
        AWSDynamoDb.get_data_from_table(dynamo_tbl_name, "key", "test_key", "us-east-1")


@mock_aws
def test_get_all_data_from_table():
    dynamo_tbl_name = "datalake-test"
    conn = boto3.resource("dynamodb", region_name="us-east-1")
    table_response = conn.create_table(
        TableName=dynamo_tbl_name,
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # Insert test data into the table
    table = conn.Table(dynamo_tbl_name)
    table.put_item(Item={"key": "test_key", "value": "test_value"})
    AWSDynamoDb.get_all_data_from_table(dynamo_tbl_name)


@mock_aws
def test_get_all_data_from_table_exception():
    dynamo_tbl_name = "datalake-test"
    with pytest.raises(AWSDynamoDbException):
        AWSDynamoDb.get_all_data_from_table(dynamo_tbl_name)


@mock_aws
def test_update_item_from_table():
    key = 'key'
    value_key = 'test_key'
    dynamo_tbl_name = "datalake-test"
    new_value = 'VALUE_UPDATED'
    conn = boto3.resource("dynamodb", region_name="us-east-1")
    table_response = conn.create_table(
        TableName=dynamo_tbl_name,
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # Insert test data into the table
    table = conn.Table(dynamo_tbl_name)
    table.put_item(Item={"key": "test_key", "value_1": "test_value"})

    update_response = AWSDynamoDb.update_item_from_table(dynamo_tbl_name, key, value_key, 'value_1', new_value)
    response = table.query(KeyConditionExpression=Key(key).eq(value_key))
    response = response.get("Items")[0]
    assert response.get('value_1') == new_value and update_response == 200


@mock_aws
def test_update_item_from_table_exception():
    dynamo_tbl_name = "datalake-test"
    with pytest.raises(AWSDynamoDbException):
        AWSDynamoDb.update_item_from_table(dynamo_tbl_name, 'dummy', 'dummy', 'dummy', 'dummy')
