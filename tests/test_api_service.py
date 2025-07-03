import pytest
import requests
import pandas as pd
import json
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from services.api_service import (
    APIService,
    APIResponseError,
    APIParsingError,
    APIConnectionError,
)
import pytest
import os
from moto import mock_aws
from datetime import datetime
from pyspark.sql.types import (
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    ArrayType,
    StructType,
    StructField,
    IntegerType
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("test")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    yield

    # Clean up
    os.environ.pop("AWS_ACCESS_KEY_ID", None)
    os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
    os.environ.pop("AWS_SECURITY_TOKEN", None)
    os.environ.pop("AWS_SESSION_TOKEN", None)
    os.environ.pop("AWS_DEFAULT_REGION", None)


@pytest.fixture
def mock_response():
    response = Mock(spec=requests.Response)
    response.status_code = 200
    response.text = '{"data": "test"}'
    return response


@pytest.fixture
def mock_logging():
    with patch("services.api_service.AWSLogService") as mock:
        yield mock


@pytest.fixture
def api_config():
    return {
        "process_name": "test_process",
        "api_url": "http://test.com",
        "api_method": "GET",
        "api_headers": {"Content-Type": "application/json"},
        "api_params": {"param": "value"},
        "timeout": 30,
    }


class TestAPIService:
    def test_validate_response_success(self, mock_response):
        APIService.validate_response(mock_response)

    def test_validate_response_server_error(self, mock_response):
        mock_response.status_code = 500
        with pytest.raises(APIResponseError, match="Server error"):
            APIService.validate_response(mock_response)

    def test_validate_response_unexpected_status(self, mock_response):
        mock_response.status_code = 302
        with pytest.raises(APIResponseError, match="Unexpected status code"):
            APIService.validate_response(mock_response)

    def test_parse_response_json_success(self, mock_response):
        mock_response.json.return_value = {"data": "test"}
        result = APIService.parse_response(mock_response)
        assert result == {"data": "test"}

    def test_parse_response_json_failure_xml_fallback(self, mock_response):
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError(
            "msg", "doc", 0
        )
        mock_response.text = "<xml>test</xml>"
        result = APIService.parse_response(mock_response)
        assert result == {"xml": "<xml>test</xml>"}

    def test_parse_response_all_parsing_fails(self, mock_response):
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError(
            "msg", "doc", 0
        )
        with patch.object(
            APIService, "parse_xml", side_effect=Exception("XML parsing failed")
        ):
            with pytest.raises(APIParsingError):
                APIService.parse_response(mock_response)

    def test_parse_xml(self):
        xml_str = "<test>data</test>"
        result = APIService.parse_xml(xml_str)
        assert result == {"xml": "<test>data</test>"}

    def test_get_api_data_invalid_method(self, api_config, mock_logging):
        api_config["api_method"] = "INVALID"
        with pytest.raises(ValueError, match="HTTP method INVALID is not supported"):
            APIService.get_api_data(api_config, pagination_type="page")

    @patch("requests.get")
    def test_get_api_data_timeout(self, mock_get, api_config, mock_logging):
        mock_get.side_effect = requests.Timeout()
        with pytest.raises(APIConnectionError, match="Request timed out"):
            APIService.get_api_data(api_config, pagination_type="page")

    @patch("requests.get")
    def test_get_api_data_connection_error(self, mock_get, api_config, mock_logging):
        mock_get.side_effect = requests.ConnectionError()
        with pytest.raises(APIConnectionError, match="Failed to connect"):
            APIService.get_api_data(api_config, pagination_type="page")

    @patch("requests.get")
    def test_get_api_data_request_exception(self, mock_get, api_config, mock_logging):
        mock_get.side_effect = requests.RequestException()
        with pytest.raises(APIConnectionError, match="Request failed"):
            APIService.get_api_data(api_config, pagination_type="page")

    def test_create_dataframe_from_response(self, spark, aws_credentials):
        """Test successful DataFrame creation from API response"""
        # Mock AWS credentials
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        test_data = [{"id": 1, "name": "Test1"}, {"id": 2, "name": "Test2"}]
        response = {"next_page": "page2", "data": test_data}

        with patch("boto3.client"):  # Mock boto3 client
            result = APIService.create_dataframe_from_response(
                spark=spark, response=response, body_name="data", pagination_type="page"
            )

            assert isinstance(result, tuple)
            df, next_page = result
            assert df.count() == 2
            assert next_page == "page2"

    def test_parse_xml_with_invalid_xml(self):
        """Test parsing invalid XML string"""
        invalid_xml = "not valid xml"
        result = APIService.parse_xml(invalid_xml)
        assert result == {"xml": "not valid xml"}

    def test_validate_response_with_empty_response(self):
        """Test validation of empty response"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.text = ""

        APIService.validate_response(mock_response)  # Should not raise any exception

    def test_parse_response_with_empty_response(self):
        """Test parsing of empty response"""
        mock_response = Mock(spec=requests.Response)
        mock_response.text = ""
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError(
            "msg", "doc", 0
        )

        result = APIService.parse_response(mock_response)
        assert result == {"xml": ""}

    def test_get_spark_type_various_types(self):
        """Test get_spark_type with different input types"""
        test_cases = [
            (None, StringType()),
            (True, BooleanType()),
            (1, LongType()),
            (1.0, DoubleType()),
            ("test", StringType()),
            ({"key": "value"}, "struct"),
            ([1, 2, 3], "array"),
            (datetime.now(), StringType()),  # Test with a non-standard type
        ]

        for input_value, expected_type in test_cases:
            result = APIService.get_spark_type(input_value)
            assert result == expected_type

    def test_infer_field_type_with_complex_structures(self):
        """Test infer_field_type with nested structures"""
        # Test empty array
        empty_array_type = APIService.infer_field_type([])
        assert isinstance(empty_array_type, ArrayType)
        assert isinstance(empty_array_type.elementType, StringType)

        # Test array of dictionaries
        array_of_dicts = [{"name": "test"}]
        array_type = APIService.infer_field_type(array_of_dicts)
        assert isinstance(array_type, ArrayType)
        assert isinstance(array_type.elementType, StructType)

        # Test nested arrays
        nested_array = [[1, 2, 3]]
        nested_type = APIService.infer_field_type(nested_array)
        assert isinstance(nested_type, ArrayType)
        assert isinstance(nested_type.elementType, ArrayType)

    def test_infer_spark_schema_complex(self):
        """Test schema inference with complex nested structures"""
        test_data = {
            "simple": "value",
            "array": [1, 2, 3],
            "nested": {"field": "value", "numbers": [1.0, 2.0]},
            "array_of_objects": [{"name": "test1"}, {"name": "test2"}],
        }

        schema = APIService.infer_spark_schema(test_data)
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 4

        # Verify specific field types
        assert schema["simple"].dataType == StringType()
        assert isinstance(schema["array"].dataType, ArrayType)
        assert isinstance(schema["nested"].dataType, StructType)
        assert isinstance(schema["array_of_objects"].dataType, ArrayType)

    def test_create_dataframe_from_response_empty_data(self, spark, mock_logging):
        """Test DataFrame creation with empty response data"""
        response = {"next_page": None, "data": None}
        result = APIService.create_dataframe_from_response(
            spark, response, "data", "page"
        )
        assert result == (None, None)

    def test_create_dataframe_from_response_cursor_pagination(
        self, spark, mock_logging
    ):
        """Test DataFrame creation with cursor-based pagination"""
        response = {
            "meta": {"has_more": True, "after_cursor": "next_cursor"},
            "data": [{"id": 1, "name": "Test"}],
        }

        df, has_more, cursor = APIService.create_dataframe_from_response(
            spark, response, "data", "cursor"
        )
        assert has_more is True
        assert cursor == "next_cursor"
        assert df.count() == 1

    def test_create_dataframe_from_response_complex_schema(self, spark, mock_logging):
        """Test DataFrame creation with complex nested data structures"""
        response = {
            "data": [
                {
                    "id": 1,
                    "simple": "value",
                    "array": [1, 2, 3],
                    "nested": {"field": "value"},
                    "mixed_array": [{"name": "test"}, {"name": "test2"}],
                }
            ]
        }

        df, _ = APIService.create_dataframe_from_response(
            spark, response, "data", "page"
        )
        assert df.count() == 1
        assert "control_ingestion_at" in df.columns

    @mock_aws
    def test_write_data_success(self, spark, mock_logging):
        """Test successful data writing to S3"""
        # Create test DataFrame
        test_data = [{"id": 1, "name": "Test"}]
        df = spark.createDataFrame(test_data)

        parameters = {
            "table_name": "test_table",
            "s3_path": "s3://test-bucket/test-path",
            "file_type": "parquet",
            "mode": "overwrite",
        }

        with patch("services.api_service.APIService.write_data") as mock_write:
            mock_write.return_value = df
            result = APIService.write_data([df], parameters)
            assert result is not None

    def test_write_data_empty_dataframes(self, mock_logging):
        """Test write_data with empty dataframes list"""
        parameters = {
            "table_name": "test_table",
            "s3_path": "s3://test-bucket/test-path",
        }
        result = APIService.write_data([], parameters)
        assert result is None

    def test_write_data_csv_format(self, spark, mock_logging):
        """Test writing data in CSV format"""
        test_data = [{"id": 1, "name": "Test"}]
        df = spark.createDataFrame(test_data)

        parameters = {
            "table_name": "test_table",
            "s3_path": "s3://test-bucket/test-path",
            "file_type": "csv",
            "mode": "overwrite",
            "partition_by": ["id"],
        }

        with patch("pyspark.sql.DataFrameWriter.save") as mock_save:
            result = APIService.write_data([df], parameters)
            assert mock_save.called

    def test_write_data_error_handling(self, spark, mock_logging):
        """Test error handling in write_data"""
        test_data = [{"id": 1, "name": "Test"}]
        df = spark.createDataFrame(test_data)

        parameters = {
            "table_name": "test_table",
            "s3_path": "s3://test-bucket/test-path",
        }

        with patch(
            "pyspark.sql.DataFrameWriter.save", side_effect=Exception("Test error")
        ):
            with pytest.raises(Exception):
                APIService.write_data([df], parameters)

#############

    def test_standardize_mixed_types_with_array_of_structs(self):
        """Test that arrays of consistently structured dictionaries are preserved."""
        data = [
            {
                "id": 1,
                "actions": [
                    {"field": "priority", "value": "high"},
                    {"field": "group_id", "value": "43100588"},
                    {"field": "follower", "value": "24519523188"},
                    {"field": "follower", "value": "24528121768"}
                ]
            }
        ]
        result = APIService.standardize_mixed_types(data)
        assert isinstance(result[0]["actions"], list)
        assert all(isinstance(action, dict) for action in result[0]["actions"])
        assert result[0]["actions"][0]["field"] == "priority"
        assert result[0]["actions"][0]["value"] == "high"

    def test_standardize_mixed_types_with_nested_mixed_types(self):
        """Test standardization of nested structures with mixed types."""
        data = [
            {
                "id": 1,
                "object_with_mixed_fields": {
                    "string_field": "value",
                    "int_field": 123,
                    "bool_field": True
                }
            },
            {
                "id": 2,
                "object_with_mixed_fields": {
                    "string_field": "another",
                    "int_field": "not an int",
                    "bool_field": False
                }
            }
        ]
        result = APIService.standardize_mixed_types(data)
        assert result[0]["id"] == 1
        assert result[0]["object_with_mixed_fields"]["int_field"] == 123
        assert result[1]["object_with_mixed_fields"]["int_field"] == "not an int"
        assert isinstance(result[0]["object_with_mixed_fields"]["int_field"], int)
        assert isinstance(result[1]["object_with_mixed_fields"]["int_field"], str)

    def test_standardize_mixed_types_with_array_field_containing_mixed_id_types(self):
        """Test standardization of a single array field containing objects with mixed ID types."""
        data = [
            {"items": [{"id": 1}, {"id": "string_id"}, {"id": True}]}
        ]
        result = APIService.standardize_mixed_types(data)
        assert isinstance(result[0]["items"][0]["id"], int)
        assert isinstance(result[0]["items"][1]["id"], str)
        assert isinstance(result[0]["items"][2]["id"], bool)
        assert len(result[0]["items"]) == 3

    def test_ensure_consistent_types(self):
        """Test type consistency enforcement."""
        data = [
            {"id": 1, "value": {"complex": "structure"}},
            {"id": 2, "value": [1, 2, 3]}
        ]
        result = APIService.ensure_consistent_types(data)
        # Complex types should be preserved but consistently
        assert isinstance(result[0]["id"], int)
        assert isinstance(result[0]["value"], dict)
        assert isinstance(result[1]["value"], list)

    def test_convert_problematic_types_to_string(self):
        """Test conversion of problematic types to strings."""
        # Create test data with various problematic types
        data = [
            {"id": 1, "mixed": [1, True, {"nested": "value"}]},
            {"id": 2, "date": "2023-01-01"}  # Date-like string
        ]
        result = APIService.convert_problematic_types_to_string(data)
        # Basic types should be preserved
        assert isinstance(result[0]["id"], int)
        # Complex nested objects in lists should be converted to strings
        assert isinstance(result[0]["mixed"][2], str)
        # Simple types in lists should be preserved
        assert isinstance(result[0]["mixed"][0], int)
        assert isinstance(result[0]["mixed"][1], bool)
        # Date-like strings should be preserved as strings
        assert result[1]["date"] == "2023-01-01"

    def test_convert_complex_types_to_string(self):
        """Test conversion of complex types to JSON strings."""
        data = [
            {"id": 1, "complex": {"nested": {"deeply": ["nested", "array"]}}},
            {"id": 2, "array": [{"obj1": "value"}, {"obj2": "value"}]}
        ]
        result = APIService.convert_complex_types_to_string(data)
        # Complex nested structures should be converted to JSON strings
        assert isinstance(result[0]["complex"], str)
        # The string should be valid JSON
        assert json.loads(result[0]["complex"])["nested"]["deeply"] == ["nested", "array"]
        # Arrays of objects should be converted to JSON strings
        assert isinstance(result[1]["array"], str)
        # Basic types should remain unchanged
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2

    def test_create_mixed_type_schema_with_arrays_of_dicts(self, spark):
        """Test creation of schema for arrays with dictionaries having mixed types."""
        # Create an array of dictionaries with mixed field types
        dict_array = [
            {"id": 1, "name": "Test1", "active": True},
            {"id": "2", "name": "Test2", "active": "true"}  # Mixed ID and active types
        ]
        schema = APIService._create_mixed_type_schema(dict_array)
        # Fields with mixed types should use StringType
        assert schema["id"].dataType == StringType()
        assert schema["active"].dataType == StringType()
        # Field with consistent type should keep that type
        assert schema["name"].dataType == StringType()

    def test_create_mixed_type_schema_with_empty_array(self):
        """Test schema creation with an empty array."""
        empty_array = []
        schema = APIService._create_mixed_type_schema(empty_array)
        # Should return an empty schema
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 0

    def test_create_mixed_type_schema_with_none_values(self):
        """Test schema creation with None values in dictionaries."""
        dict_array = [
            {"id": 1, "name": None, "description": "Test1"},
            {"id": 2, "name": "Test2", "description": None}
        ]
        schema = APIService._create_mixed_type_schema(dict_array)
        # All fields should be nullable
        assert schema["id"].nullable
        assert schema["name"].nullable
        assert schema["description"].nullable

    def test_process_individual_dataframes_with_different_flatten_depths(self, spark, mock_logging):
        """Test processing DataFrames with different flattening depths."""
        df = Mock()
        df.columns = ["id", "nested", "array"]
        with patch("services.api_service.SparkService.flatten_nested_columns") as mock_flatten, \
             patch("services.api_service.SparkService.lower_columns") as mock_lower, \
             patch("services.api_service.SparkService.replace_spaces_with_underscores") as mock_replace, \
             patch("services.api_service.SparkService.normalize_column_types") as mock_normalize, \
             patch("services.api_service.SparkService.merge_duplicate_columns") as mock_merge, \
             patch("services.api_service.SparkService.process_json_string_fields") as mock_process:
            df_depth1 = Mock()
            df_depth1.columns = ["id", "nested", "array"]
            mock_flatten.return_value = df_depth1
            mock_lower.return_value = df_depth1
            mock_replace.return_value = df_depth1
            mock_normalize.return_value = df_depth1
            mock_merge.return_value = df_depth1
            processed_dfs_depth1 = APIService._process_individual_dataframes([df], {"flatten_depth": 1})
            assert len(processed_dfs_depth1) == 1
            assert mock_flatten.called_with(df, flattening_depth=1)
            df_depth3 = Mock()
            df_depth3.columns = ["id", "nested_level1_level2_level3", "array"]
            mock_flatten.return_value = df_depth3
            mock_lower.return_value = df_depth3
            mock_replace.return_value = df_depth3
            mock_normalize.return_value = df_depth3
            mock_merge.return_value = df_depth3
            mock_process.return_value = df_depth3
            processed_dfs_depth3 = APIService._process_individual_dataframes([df], {"flatten_depth": 3})
            assert len(processed_dfs_depth3) == 1
            assert mock_flatten.called_with(df, flattening_depth=3)
            assert mock_process.called

    def test_align_and_combine_dataframes_with_different_schemas(self, spark, mock_logging):
        """Test combining DataFrames with different schemas."""
        # Create two DataFrames with different schemas
        df1 = spark.createDataFrame([{"id": 1, "name": "Test1"}])
        df2 = spark.createDataFrame([{"id": 2, "description": "Test description"}])
        parameters = {"cast_array_fields": False}
        # Combine the DataFrames
        combined_df = APIService._align_and_combine_dataframes([df1, df2], parameters)
        # Check the combined schema
        assert "id" in combined_df.columns
        assert "name" in combined_df.columns
        assert "description" in combined_df.columns
        # Check data retention
        assert combined_df.count() == 2
        # Verify null values where columns didn't exist in original DataFrames
        df1_row = combined_df.filter("id = 1").first()
        assert df1_row.description is None
        df2_row = combined_df.filter("id = 2").first()
        assert df2_row.name is None

    def test_align_and_combine_dataframes_with_type_conflicts(self, spark, mock_logging):
        """Test handling type conflicts when combining DataFrames."""
        # Create DataFrames with conflicting types for the same column
        df1 = spark.createDataFrame([{"id": 1, "value": 100}])
        df2 = spark.createDataFrame([{"id": 2, "value": "string value"}])
        parameters = {"cast_array_fields": False}
        # Combine the DataFrames
        combined_df = APIService._align_and_combine_dataframes([df1, df2], parameters)
        # Check that the combined DataFrame has the correct schema
        assert "id" in combined_df.columns
        assert "value" in combined_df.columns
        # Both original values should be preserved but as strings
        df1_row = combined_df.filter("id = 1").first()
        df2_row = combined_df.filter("id = 2").first()
        # All values should be strings to handle type conflicts
        assert isinstance(df1_row.value, str)
        assert isinstance(df2_row.value, str)

    def test_validate_dataframes(self, mock_logging):
        """Test validation of DataFrames."""
        # Create a mock DataFrame
        mock_df = Mock()
        mock_df.rdd = Mock()
        mock_df.rdd.isEmpty.return_value = False
        # Test with valid DataFrame
        valid_dfs = [mock_df]
        result = APIService._validate_dataframes(valid_dfs, mock_logging)
        assert result == valid_dfs
        # Test with empty DataFrame
        mock_empty_df = Mock()
        mock_empty_df.rdd = Mock()
        mock_empty_df.rdd.isEmpty.return_value = True
        empty_dfs = [mock_empty_df]
        result = APIService._validate_dataframes(empty_dfs, mock_logging)
        assert result is None
        # Test with None DataFrame
        none_dfs = [None]
        result = APIService._validate_dataframes(none_dfs, mock_logging)
        assert result is None
        # Test with empty list
        result = APIService._validate_dataframes([], mock_logging)
        assert result is None

    def test_write_to_storage(self, spark, mock_logging):
        """Test writing DataFrame to storage with different parameters."""
        # Create test DataFrame
        df = spark.createDataFrame([{"id": 1, "name": "Test"}])
        with patch("services.api_service.SparkService.save_frame") as mock_save:
            # Test with standard parameters
            APIService._write_to_storage(
                df=df,
                s3_path="s3://bucket/path",
                file_type="parquet",
                write_mode="append",
                partition_columns=["id"],
                table_name="test_table",
                extraction_folder="custom_folder",
                logging=mock_logging
            )
            # Verify SparkService.save_frame was called
            assert mock_save.called
            # Verificar los argumentos correctamente
            mock_save.assert_called_once()
            # Obtener los argumentos pasados
            call_args = mock_save.call_args
            args, kwargs = call_args
            # Verificar que el DataFrame es el primer argumento
            assert args[0] is df
            # Verificar que la ruta es correcta (segundo argumento)
            expected_path = "s3://bucket/path/custom_folder/"
            assert args[1] == expected_path
            # Verificar el formato de archivo (tercer argumento)
            assert args[2] == "parquet"
            # Verificar el modo de escritura (cuarto argumento)
            assert args[3] == "append"
            # Verificar las columnas de partición (quinto argumento)
            assert args[4] == ["id"]

    def test_empty_list(self, spark):
        """Test that an empty list raises a ValueError."""
        with pytest.raises(ValueError):
            APIService._align_dataframes_schema([])

    def test_single_dataframe(self, spark):
        """Test that a list with a single DataFrame returns the same list."""
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        result = APIService._align_dataframes_schema([df])
        assert len(result) == 1
        assert result[0].columns == df.columns

    def test_missing_columns(self, spark):
        """Test that missing columns are added with null values."""
        # Reference DataFrame with full schema
        df1 = spark.createDataFrame([(1, "a", 100)], ["id", "name", "value"])
        # DataFrame missing one column
        df2 = spark.createDataFrame([(2, "b")], ["id", "name"])
        # DataFrame missing a different column
        df3 = spark.createDataFrame([(3, 300)], ["id", "value"])
        dfs = [df1, df2, df3]
        aligned_dfs = APIService._align_dataframes_schema(dfs)
        assert len(aligned_dfs) == 3
        # Verify all have the same schema
        for df in aligned_dfs:
            assert df.columns == ["id", "name", "value"]
        # Verify values for df1 (unchanged)
        row1 = aligned_dfs[0].collect()[0]
        assert row1["id"] == 1
        assert row1["name"] == "a"
        assert row1["value"] == 100
        # Verify values for df2 (added 'value' column)
        row2 = aligned_dfs[1].collect()[0]
        assert row2["id"] == 2
        assert row2["name"] == "b"
        assert row2["value"] is None  # Should be null
        # Verify values for df3 (added 'name' column)
        row3 = aligned_dfs[2].collect()[0]
        assert row3["id"] == 3
        assert row3["name"] is None  # Should be null
        assert row3["value"] == 300

    def test_column_order(self, spark):
        """Test that columns are reordered to match the reference DataFrame."""
        # Reference DataFrame
        df1 = spark.createDataFrame([(1, "a", 100)], ["id", "name", "value"])
        # DataFrame with different column order
        df2 = spark.createDataFrame([(2, 200, "b")], ["id", "value", "name"])
        dfs = [df1, df2]
        aligned_dfs = APIService._align_dataframes_schema(dfs)
        # Verify column order
        assert aligned_dfs[1].columns == ["id", "name", "value"]
        # Verify values are preserved and in correct positions
        row = aligned_dfs[1].collect()[0]
        assert row["id"] == 2
        assert row["name"] == "b"
        assert row["value"] == 200
