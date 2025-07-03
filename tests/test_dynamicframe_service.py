import pytest
import boto3
from moto import mock_aws
from unittest.mock import Mock, patch
from pyspark.sql.utils import AnalysisException, StreamingQueryException
from py4j.protocol import Py4JJavaError

from services.dynamicframe_service import DynamicFrameService, DynamicFrameServiceException

@pytest.fixture
def mock_glue_context():
    """Create a mock Glue context."""
    return Mock()

@pytest.fixture
def mock_data_frame():
    """Create a mock DataFrame."""
    return Mock()

@pytest.fixture
def aws_services(aws_credentials):
    """Set up AWS service clients."""
    with mock_aws():
        glue_client = boto3.client('glue', region_name='us-east-1')
        logs_client = boto3.client('logs', region_name='us-east-1')
        # Create mock resources
        database_name = "test_database"
        table_name = "test_table"
        log_group_name = "test-log-group"
        # Create log group
        logs_client.create_log_group(logGroupName=log_group_name)
        # Create database
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        # Create table
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'column1', 'Type': 'string'},
                        {'Name': 'column2', 'Type': 'int'}
                    ],
                    'Location': 's3://test-bucket/test-path'
                }
            }
        )
        yield {
            'glue_client': glue_client,
            'logs_client': logs_client,
            'database_name': database_name,
            'table_name': table_name,
            'log_group_name': log_group_name
        }
        # Cleanup
        try:
            glue_client.delete_table(
                DatabaseName=database_name,
                Name=table_name
            )
            glue_client.delete_database(
                Name=database_name
            )
            logs_client.delete_log_group(
                logGroupName=log_group_name
            )
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")

def test_read_data_frame_from_options_success(mock_glue_context, mock_data_frame):
    """Test successful reading of dataframe using from_options."""
    # Arrange
    parameters = {
        'connection_type': 'kinesis',
        'connection_options': {
            'streamName': 'test-stream',
            'region': 'us-east-1'
        },
        'transformation_ctx': 'test_ctx'
    }
    mock_glue_context.create_data_frame.from_options.return_value = mock_data_frame
    # Act
    result = DynamicFrameService.read_data_frame_from_options(
        mock_glue_context,
        parameters
    )
    # Assert
    assert result == mock_data_frame
    mock_glue_context.create_data_frame.from_options.assert_called_once_with(
        connection_type=parameters['connection_type'],
        connection_options=parameters['connection_options'],
        transformation_ctx=parameters['transformation_ctx']
    )

def test_read_data_frame_from_options_error():
    """Test error handling when reading dataframe using from_options."""
    # Arrange
    parameters = {}
    # Act & Assert
    with pytest.raises(DynamicFrameServiceException):
        DynamicFrameService.read_data_frame_from_options(None, parameters)

def test_read_data_frame_from_catalog_success(mock_glue_context, mock_data_frame, aws_services):
    """Test successful reading of dataframe from catalog."""
    # Arrange
    parameters = {
        'database': aws_services['database_name'],
        'table_name': aws_services['table_name'],
        'predicate': '',
        'transformation_ctx': 'test_ctx'
    }
    additional_read_options = {'option1': 'value1'}
    mock_glue_context.create_dynamic_frame.from_catalog.return_value = mock_data_frame
    # Act
    result = DynamicFrameService.read_data_frame_from_catalog(
        mock_glue_context,
        parameters,
        additional_read_options
    )
    # Assert
    assert result == mock_data_frame
    mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once_with(
        database=parameters['database'],
        table_name=parameters['table_name'],
        predicate=parameters['predicate'],
        additional_options=additional_read_options,
        transformation_ctx=parameters['transformation_ctx']
    )

def test_read_data_frame_from_catalog_error():
    """Test error handling when read data using data catalog."""
    # Arrange
    parameters = {
        'database': "db_error",
        'table_name': "table_error",
        'predicate': '',
        'transformation_ctx': 'test_ctx'
    }
    # Act & Assert
    with pytest.raises(DynamicFrameServiceException):
        DynamicFrameService.read_data_frame_from_catalog(None, parameters, None)

@patch('services.dynamicframe_service.AWSLogService')
def test_write_data_frame_from_options_success(mock_aws_log, mock_glue_context, mock_data_frame):
    """Test successful writing of dataframe using from_options."""
    # Arrange
    parameters = {
        'connection_type': 's3',
        'connection_options': {
            'path': 's3://test-bucket/output/',
            'partitionKeys': ['year', 'month']
        },
        'transformation_ctx': 'test_ctx'
    }
    # Act
    DynamicFrameService.write_data_frame_from_options(
        mock_glue_context,
        mock_data_frame,
        parameters
    )
    # Assert
    mock_glue_context.write_dynamic_frame.from_options.assert_called_once_with(
        frame=mock_data_frame,
        connection_type=parameters['connection_type'],
        connection_options=parameters['connection_options'],
        transformation_ctx=parameters['transformation_ctx']
    )

def test_write_data_frame_from_options_error():
    """Test error handling when writing dataframe using from_options."""
    # Arrange
    parameters = {
        'connection_type': 's3',
        'connection_options': {
            'path': 's3://non-existent-bucket/output/',
            'partitionKeys': ['year', 'month']
        },
        'transformation_ctx': 'test_ctx'
    }
    # Act & Assert
    with pytest.raises(DynamicFrameServiceException):
        DynamicFrameService.write_data_frame_from_options(
            None,
            None,
            parameters
        )
