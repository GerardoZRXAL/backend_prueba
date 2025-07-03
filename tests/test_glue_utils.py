import pytest
import boto3
from moto import mock_aws
from aws_utils.glue_utils import GlueUtils
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError, ParamValidationError


@mock_aws
def test_get_table():
    client = boto3.client("glue", region_name="us-east-1")
    response = client.create_database(
        DatabaseInput={
            "Name": "test",
        }
    )
    response = client.create_table(
        DatabaseName="test",
        TableInput={
            "Name": "test_table",
        },
    )
    GlueUtils.get_table("test", "test_table")


@mock_aws
def test_update_table():
    client = boto3.client("glue", region_name="us-east-1")
    client.create_database(
        DatabaseInput={
            "Name": "test",
        }
    )
    client.create_table(
        DatabaseName="test",
        TableInput={
            "Name": "test_table",
        },
    )
    table_input = {"Name": "test_table", "Description": "some description"}
    GlueUtils.update_table("test", table_input)


@pytest.fixture
def mock_boto3_client():
    with patch('boto3.client') as mock:
        mock_client = MagicMock()
        mock.return_value = mock_client
        yield mock_client

def test_delete_old_table_versions_successful(mock_boto3_client):
    # Configurar las versiones simuladas
    mock_versions = {
        'TableVersions': [
            {'VersionId': str(i), 'Table': {}} for i in range(1, 16)  # Crea 15 versiones
        ]
    }
    mock_boto3_client.get_table_versions.return_value = mock_versions

    # Configurar respuesta exitosa para batch_delete_table_version
    mock_boto3_client.batch_delete_table_version.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200}
    }

    result = GlueUtils.delete_old_table_versions(
        database_name="test_db",
        table_name="test_table",
        versions_to_keep=10
    )

    # Verificaciones
    assert result == 'SUCCESS'


def test_delete_old_table_versions_with_pagination(mock_boto3_client):
    # Simular paginación con NextToken
    mock_boto3_client.get_table_versions.side_effect = [
        {
            'TableVersions': [{'VersionId': str(i), 'Table': {}} for i in range(1, 11)],
            'NextToken': 'token123'
        },
        {
            'TableVersions': [{'VersionId': str(i), 'Table': {}} for i in range(11, 16)],
        }
    ]

    result = GlueUtils.delete_old_table_versions(
        database_name="test_db",
        table_name="test_table",
        versions_to_keep=10
    )

    assert result == 'SUCCESS'


def test_delete_old_table_versions_param_validation_error(mock_boto3_client):
    # Simular error de validación de parámetros
    mock_boto3_client.get_table_versions.side_effect = ParamValidationError(
        report='Invalid parameters'
    )

    result = GlueUtils.delete_old_table_versions(
        database_name="test_db",
        table_name="test_table"
    )
    assert "error" in result

def test_delete_old_table_versions_batch_error(mock_boto3_client):
    # Configurar versiones exitosas pero error en el borrado
    mock_boto3_client.get_table_versions.return_value = {
        'TableVersions': [{'VersionId': str(i), 'Table': {}} for i in range(1, 16)]
    }

    # Simular error en batch_delete_table_version
    mock_boto3_client.batch_delete_table_version.side_effect = ClientError(
        error_response={
            'Error': {
                'Code': 'ValidationException',
                'Message': 'Cannot delete versions'
            }
        },
        operation_name='BatchDeleteTableVersion'
    )

    result = GlueUtils.delete_old_table_versions(
        database_name="test_db",
        table_name="test_table",
        versions_to_keep=10
    )

    assert result == 'SUCCESS'
