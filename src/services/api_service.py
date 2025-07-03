import json
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict
from typing import List
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    BooleanType,
    LongType,
    DoubleType,
    MapType,
)
from pyspark.sql.utils import AnalysisException
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from loggers.aws_log_service import AWSLogService
from services.spark_service import SparkService


class APIResponseError(Exception):
    pass


class APIServiceException(Exception):
    pass


class APIParsingError(Exception):
    pass


class APIConnectionError(Exception):
    pass



class APIService:
    @staticmethod
    def validate_response(response: requests.Response) -> None:
        """
        Validates the HTTP response from an API call.
        """
        if response.status_code >= 500:
            raise APIResponseError(
                f"Server error: {response.status_code} - {response.text}"
            )
        if response.status_code >= 400:
            try:
                error_data = response.json()
                if "errors" in error_data:
                    return None
            except JSONDecodeError as exc:
                # Add 'from exc' to properly chain the exception
                raise APIResponseError(
                    f"Client error: {response.status_code} - {response.text}"
                ) from exc  # This is the change needed
        if response.status_code != 200:
            raise APIResponseError(
                f"Unexpected status code: {response.status_code} - {response.text}"
            )
        return True

    @staticmethod
    def parse_response(response: requests.Response) -> Dict:
        """
        Parses the response content from an API call into a dictionary.

        Args:
            response (requests.Response): The response object from the HTTP request

        Returns:
            Dict: The parsed response data

        Raises:
            APIParsingError: If the response cannot be parsed as JSON or XML
        """
        try:
            return response.json()
        except JSONDecodeError:
            try:
                return APIService.parse_xml(response.text)
            except Exception as exc:
                raise APIParsingError(
                    f"Failed to parse response as JSON or XML: {str(exc)}"
                ) from exc

    @staticmethod
    def parse_xml(xml_str: str) -> Dict:
        """
        Parses an XML string into a dictionary format.

        Args:
            xml_str (str): The XML string to parse

        Returns:
            Dict: A dictionary containing the parsed XML data with 'xml' as the key

        Raises:
            APIParsingError: If the XML string cannot be parsed
        """
        try:
            return {"xml": xml_str}
        except Exception as exc:
            raise APIParsingError(f"Failed to parse XML: {str(exc)}") from exc

    @staticmethod
    def get_api_data(api_config, pagination_type):
        """
        Retrieves data from an API endpoint based on the provided configuration.

        Args:
            api_config: Configuration dictionary containing API details including:
                - process_name: Name of the process
                - api_method: HTTP method (GET, POST, PUT, DELETE)
                - api_url: The endpoint URL
                - api_headers: Request headers
                - api_params: Query parameters
                - api_body: Request body for POST/PUT
                - timeout: Request timeout in seconds
                - auth: Authentication configuration
            pagination_type: Type of pagination ('cursor' or other)

        Returns:
            Dict: The parsed response data from the API

        Raises:
            APIConnectionError: If there are connection issues
            APIResponseError: If the API returns an error response
            ValueError: If an unsupported HTTP method is specified
        """
        logging = AWSLogService()

        process_name = api_config.get("process_name")
        api_method = api_config.get("api_method", "GET").upper()
        api_url = api_config.get("api_url")
        api_headers = api_config.get("api_headers", {})
        api_params = api_config.get("api_params", {})
        api_body = api_config.get("api_body", None)
        timeout = int(float(api_config.get("timeout", 30)))

        if pagination_type == "cursor":
            api_params.pop("page", None)

        after_token = api_params.get("after_token")
        if after_token:
            api_params["page[after]"] = after_token
            api_params.pop("after_token", None)

        request_kwargs = {
            "url": api_url,
            "headers": api_headers,
            "params": api_params,
            "timeout": timeout,
        }

        if api_config.get("auth"):
            auth_config = api_config["auth"]
            if auth_config["auth_type"] == "basic":
                request_kwargs["auth"] = HTTPBasicAuth(
                    auth_config["credentials"]["email"],
                    auth_config["credentials"]["token"],
                )
                logging.log_info(
                    {
                        "module": "APIService.get_api_data",
                        "log_output_msg": "Added basic authentication",
                        "process_name": process_name,
                        "@LEVEL": "DEBUG",
                    }
                )
            elif auth_config["auth_type"] == "bearer_token":
                token_bearer = auth_config["credentials"]["token"]
                request_kwargs["headers"] = {"Authorization": f"Bearer {token_bearer}"}
                logging.log_info(
                    {
                        "module": "APIService.get_api_data",
                        "log_output_msg": "Added bearer token authentication",
                        "process_name": process_name,
                        "@LEVEL": "DEBUG",
                    }
                )

        try:
            if api_method == "GET":
                response = requests.get(**request_kwargs)
            elif api_method == "POST":
                request_kwargs["json"] = api_body
                response = requests.post(**request_kwargs)
            elif api_method == "PUT":
                request_kwargs["json"] = api_body
                response = requests.put(**request_kwargs)
            elif api_method == "DELETE":
                response = requests.delete(**request_kwargs)
            else:
                raise ValueError(f"HTTP method {api_method} is not supported")

            validation_result = APIService.validate_response(response)
            if validation_result is None:
                logging.log_info(
                    {
                        "module": "APIService.get_api_data",
                        "log_output_msg": "API response validation returned None",
                        "status": "Completed",
                        "@LEVEL": "DEBUG",
                    }
                )
                return None
            parsed_response = APIService.parse_response(response)
            data = response.json()

            if pagination_type == "cursor":
                meta = data.get("meta", {})
                parsed_response["has_more"] = meta.get("has_more", False)
                parsed_response["after_cursor"] = meta.get("after_cursor")

            logging.log_info(
                {
                    "module": "APIService.get_api_data",
                    "log_output_msg": "Obtained data correctly from API",
                    "status": "Completed",
                    "@LEVEL": "DEBUG",
                }
            )
            return parsed_response

        except requests.Timeout as exc:
            logging.log_error(
                {
                    "module": "ApiService.get_api_data",
                    "log_output_msg": f"Request timed out after {timeout} seconds",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise APIConnectionError(
                f"Request timed out after {timeout} seconds"
            ) from exc
        except requests.ConnectionError as exc:
            logging.log_error(
                {
                    "module": "ApiService.get_api_data",
                    "log_output_msg": f"Failed to connect to {api_url}: {str(exc)}",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise APIConnectionError(
                f"Failed to connect to {api_url}: {str(exc)}"
            ) from exc
        except RequestException as exc:
            logging.log_error(
                {
                    "module": "ApiService.get_api_data",
                    "log_output_msg": f"Request failed: {str(exc)}",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise APIConnectionError(f"Request failed: {str(exc)}") from exc

    @staticmethod
    def get_spark_type(value):
        """
        Determines the appropriate Spark data type for a given Python value.

        Args:
            value: The Python value to analyze for Spark type conversion

        Returns:
            pyspark.sql.types: The corresponding Spark data type:
                - StringType for None or strings
                - BooleanType for boolean values
                - LongType for integers
                - DoubleType for floating point numbers
                - StringType as default for other types
        """
        if value is None:
            return StringType()
        if isinstance(value, bool):
            return BooleanType()
        if isinstance(value, int):
            return LongType()
        if isinstance(value, float):
            return DoubleType()
        if isinstance(value, str):
            return StringType()
        if isinstance(value, dict):
            # If empty dict, return MapType with string keys and values
            if not value:
                return MapType(StringType(), StringType(), True)
            return "struct"
        if isinstance(value, list):
            # If empty list, return ArrayType with string elements
            if not value:
                return ArrayType(StringType(), True)
            return "array"

        return StringType()

    @staticmethod
    def infer_field_type(value):
        """Infer the type of a field, handling nested structures and mixed array types"""
        if value is None:
            return StringType()

        # Handle empty collections
        if isinstance(value, dict) and not value:
            return MapType(StringType(), StringType(), True)

        if isinstance(value, list) and not value:
            return ArrayType(StringType(), True)

        # For non-empty values, continue with regular type detection
        basic_type = APIService.get_spark_type(value)

        if basic_type == "struct":
            return APIService.infer_spark_schema(value)

        if basic_type == "array":
            # Para arrays, primero analizar los tipos de todos los elementos
            if len(value) > 0:
                # Caso especial: array de diccionarios
                if all(isinstance(item, dict) for item in value):
                    # Verificar si hay tipos mixtos en campos comunes
                    common_fields = set.intersection(*[set(item.keys()) for item in value])
                    # Para cada campo común, verificar si tiene tipos mixtos
                    has_mixed_types = False
                    for field in common_fields:
                        field_types = set()
                        for item in value:
                            # Omitir None values
                            if item[field] is not None:
                                if isinstance(item[field], (int, float, bool, str)):
                                    field_types.add(type(item[field]).__name__)
                                else:
                                    field_types.add("complex")
                        # Si hay múltiples tipos, tenemos tipos mixtos
                        if len(field_types) > 1:
                            has_mixed_types = True
                            break
                    # Si hay tipos mixtos, inferir cada elemento individualmente
                    if has_mixed_types:
                        # Crear un schema común usando StringType para campos problemáticos
                        combined_schema = APIService._create_mixed_type_schema(value)
                        return ArrayType(combined_schema)
                    # Si no hay tipos mixtos, usar el enfoque normal
                    element_schema = APIService.infer_spark_schema(value[0])
                    return ArrayType(element_schema)
                # Para arrays de tipos simples
                element_types = set()
                for item in value:
                    if item is not None:
                        if isinstance(item, (int, float, bool, str)):
                            element_types.add(type(item).__name__)
                        else:
                            element_types.add("complex")
                # Si hay tipos mixtos en el array, usar StringType
                if len(element_types) > 1:
                    return ArrayType(StringType(), True)
                # De lo contrario, usar el tipo normal
                element_type = APIService.get_spark_type(value[0])
                if element_type == "struct":
                    return ArrayType(APIService.infer_spark_schema(value[0]))
                if element_type == "array":
                    nested_type = APIService.infer_field_type(value[0])
                    return ArrayType(nested_type)
                return ArrayType(element_type)
            # Fallback para arrays vacíos
            return ArrayType(StringType(), True)
        return basic_type


    @staticmethod
    def infer_spark_schema(data):
        """Recursively infer Spark schema from JSON-like data"""
        if isinstance(data, dict):
            fields = []
            for key, value in data.items():
                field_type = APIService.infer_field_type(value)
                fields.append(StructField(key, field_type, True))
            return StructType(fields)
        return StringType()


    @staticmethod
    def create_dataframe_from_response(spark, response, body_name, pagination_type):
        """
        Creates a Spark DataFrame from an API response data with consistent type handling.
        Optimized for performance and compatibility with mixed data types.
        """
        logging = AWSLogService()
        try:
            next_page = response.get("next_page")
            json_data = response.get(body_name)
            has_more = response.get("meta", {}).get("has_more", False)
            after_cursor = response.get("meta", {}).get("after_cursor", None)
            if not json_data:
                if pagination_type == "cursor":
                    return None, has_more, after_cursor
                return None, next_page
            # Convert data to list if it's not already
            if not isinstance(json_data, list):
                json_data = [json_data]
            # Optimización: Procesar los datos una sola vez, manteniendo estructuras anidadas
            processed_records = []
            for record in json_data:
                if isinstance(record, dict):
                    # Mantener estructuras anidadas pero sanitizar tipos numéricos/boolean
                    safe_record = {}
                    for key, value in record.items():
                        if isinstance(value, dict):
                            # Preservar diccionarios para el aplanamiento
                            safe_record[key] = value
                        elif isinstance(value, list) and value and isinstance(value[0], dict):
                            # Preservar listas de diccionarios para aplanamiento
                            safe_record[key] = value
                        elif isinstance(value, bool):
                            # Convertir valores booleanos a strings para evitar conflictos de tipo
                            safe_record[key] = str(value)
                        else:
                            # Mantener otros valores tal cual
                            safe_record[key] = value
                    processed_records.append(safe_record)
                else:
                    # Convertir valores no diccionario a formato compatible
                    processed_records.append({"value": str(record) if record is not None else None})
            rdd = spark.sparkContext.parallelize(processed_records)
            try:
                # Corregido: Eliminar la lambda innecesaria y pasar directamente json_dumps como función
                df = spark.read.json(rdd.map(json.dumps))
                # Añadir timestamp de ingesta
                df = df.withColumn(
                    "control_ingestion_at", F.current_timestamp().cast("string")
                )
                logging.log_info(
                    {
                        "module": "APIService.create_dataframe_from_response",
                        "log_output_msg": f"Created DataFrame with {df.count()} rows",
                        "status": "Completed",
                        "@LEVEL": "DEBUG",
                    }
                )
                if pagination_type == "cursor":
                    return df, has_more, after_cursor
                return df, next_page
            # Corregido: Usar excepciones específicas en lugar de Exception genérica
            except (ValueError, TypeError, AnalysisException) as json_error:
                logging.log_error(
                    {
                        "module": "APIService.create_dataframe_from_response",
                        "log_output_msg": f"Error creating DataFrame: {str(json_error)}. Trying fallback approach.",
                        "status": "Warning",
                        "@LEVEL": "ERROR",
                    }
                )
                # Enfoque alternativo: Convertir todos los valores a strings
                string_records = []
                for record in json_data:
                    if isinstance(record, dict):
                        # Convertir todos los valores a string, excepto diccionarios y listas de diccionarios
                        string_record = {}
                        for key, value in record.items():
                            if isinstance(value, dict):
                                # Procesar diccionarios internos
                                processed_dict = {}
                                for k, v in value.items():
                                    processed_dict[k] = str(v) if v is not None else None
                                string_record[key] = processed_dict
                            elif isinstance(value, list) and value and isinstance(value[0], dict):
                                # Procesar listas de diccionarios
                                processed_list = []
                                for item in value:
                                    if isinstance(item, dict):
                                        processed_item = {k: str(v) if v is not None else None for k, v in item.items()}
                                        processed_list.append(processed_item)
                                    else:
                                        processed_list.append(str(item) if item is not None else None)
                                string_record[key] = processed_list
                            else:
                                # Convertir valores simples a string
                                string_record[key] = str(value) if value is not None else None
                        string_records.append(string_record)
                    else:
                        # Convertir valores no diccionario
                        string_records.append({"value": str(record) if record is not None else None})
                # Crear RDD y DataFrame
                string_rdd = spark.sparkContext.parallelize(string_records)
                # Corregido: Eliminar la lambda innecesaria y pasar directamente json_dumps como función
                df = spark.read.json(string_rdd.map(json.dumps))
                # Añadir timestamp de ingesta
                df = df.withColumn(
                    "control_ingestion_at", F.current_timestamp().cast("string")
                )
                logging.log_info(
                    {
                        "module": "APIService.create_dataframe_from_response",
                        "log_output_msg": f"Created DataFrame with string conversion with {df.count()} rows",
                        "status": "Completed",
                        "@LEVEL": "DEBUG",
                    }
                )
                if pagination_type == "cursor":
                    return df, has_more, after_cursor
                return df, next_page

        except Exception as e:
            logging.log_error(
                {
                    "module": "APIService.create_dataframe_from_response",
                    "error_msg": f"Error creating DataFrame: {str(e)}",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise

    @staticmethod
    def _process_individual_dataframes(dataframes, parameters):
        """
        Normalizes and transforms a collection of DataFrames through a multi-step standardization process.

        This method applies a sequence of data quality transformations to each DataFrame individually,
        including: flattening nested structures based on configurable depth, standardizing column names,
        normalizing data types, resolving duplicate columns, and optionally processing JSON string fields
        when the flattening depth is sufficient (≥3).

        Args:
            dataframes (list): List of DataFrames to process
            parameters (dict): Processing parameters including flattening_depth

        Returns:
            list: List of processed DataFrames
        """
        logging = AWSLogService()
        processed_dataframes = []
        # Get flattening depth from parameters with default fallback
        # Asegurar que flatten_depth sea un entero
        flattening_depth = parameters.get("flatten_depth", 2)
        try:
            flattening_depth = int(flattening_depth)
        except (ValueError, TypeError):
            flattening_depth = 2
        logging.log_info(
            {
                "module": "APIService._process_individual_dataframes",
                "log_output_msg": f"Processing dataframes with flatten_depth: {flattening_depth}, type: {type(flattening_depth)}",
                "@LEVEL": "INFO",
            }
        )
        for df in dataframes:
            # Step 1: Flatten nested structures with configurable depth from parameters
            flattened_df = SparkService.flatten_nested_columns(
                df,
                flattening_depth=flattening_depth
            )

            # Step 2: Normalize column names
            normalized_df = SparkService.lower_columns(flattened_df)
            normalized_df = SparkService.replace_spaces_with_underscores(normalized_df)

            # Step 3: Normalize data types
            normalized_df = SparkService.normalize_column_types(normalized_df)

            # Step 4: Combine duplicate columns
            normalized_df = SparkService.merge_duplicate_columns(normalized_df)
            # Step 5: Process JSON string fields solo si el nivel de aplanamiento es 3 o mayor
            if flattening_depth >= 3:
                logging.log_info(
                    {
                        "module": "APIService._process_individual_dataframes",
                        "log_output_msg": f"Applying JSON string field processing due to flatten_depth={flattening_depth}",
                        "@LEVEL": "INFO",
                    }
                )
                normalized_df = SparkService.process_json_string_fields(normalized_df)
            else:
                logging.log_info(
                    {
                        "module": "APIService._process_individual_dataframes",
                        "log_output_msg": f"Omitting processing of JSON string fields due to flatten_depth={flattening_depth}",
                        "@LEVEL": "INFO",
                    }
                )

            processed_dataframes.append(normalized_df)

        return processed_dataframes

    @staticmethod
    def write_data(dataframes, parameters):
        """
        Main method to write multiple DataFrames to S3.
        """
        logging = AWSLogService()

        try:
            # Initial validation
            valid_dataframes = APIService._validate_dataframes(dataframes, logging)
            if not valid_dataframes:
                return None

            # Validate s3_path parameter
            s3_path = parameters.get("s3_path")
            if s3_path is None:
                bucket_name = parameters.get("bucket_name")
                prefix_path = parameters.get("prefix_path", "")
                if bucket_name:
                    s3_path = f"s3://{bucket_name}/{prefix_path}"
                    parameters["s3_path"] = s3_path
                    logging.log_info({
                        "module": "APIService.write_data",
                        "log_output_msg": f"Constructed s3_path from bucket_name and prefix_path: {s3_path}",
                        "status": "Running",
                        "@LEVEL": "DEBUG",
                    })
                else:
                    error_msg = "Missing required s3_path parameter and unable to construct it from bucket_name and prefix_path"
                    logging.log_error({
                        "module": "APIService.write_data",
                        "log_output_msg": f"***** Error: {error_msg} *****",
                        "status": "Error",
                        "@LEVEL": "ERROR",
                    })
                    raise APIServiceException(error_msg)

            # Individual processing of dataframes - passing all parameters
            processed_dataframes = APIService._process_individual_dataframes(valid_dataframes, parameters)

            # Align schemas between DataFrames
            processed_dataframes = APIService._align_dataframes_schema(processed_dataframes)
            # Schema alignment
            combined_df = APIService._align_and_combine_dataframes(processed_dataframes, parameters)

            # Data writing parameters
            table_name = parameters.get("table_name")
            s3_path = s3_path.rstrip("/")  # Safe to call rstrip now
            file_type = parameters.get("file_type", "parquet")
            write_mode = parameters.get("mode", "append")
            partition_columns = parameters.get("partition_by", None)
            extraction_folder = parameters.get("extraction_folder", "extraction")

            APIService._write_to_storage(
                combined_df,
                s3_path,
                file_type,
                write_mode,
                partition_columns,
                table_name,
                extraction_folder,
                logging
            )

            return combined_df

        except Exception as e:
            logging.log_error({
                "module": "APIService.write_data",
                "log_output_msg": f"***** Error: {e} *****",
                "status": "Error",
                "@LEVEL": "ERROR",
            })
            raise APIServiceException(f"Error writing data: {str(e)}") from e

    @staticmethod
    def _validate_dataframes(dataframes, logging):
        """
        Validates and filters empty DataFrames.

        Args:
            dataframes (list): List of DataFrames to validate
            logging: Logging object

        Returns:
            list: List of valid DataFrames
        """
        if not dataframes:
            logging.log_info({
                "module": "APIService._validate_dataframes",
                "log_output_msg": "No DataFrames to write",
                "status": "Skipped",
                "@LEVEL": "INFO",
            })
            return None

        valid_dataframes = [df for df in dataframes if df is not None and not df.rdd.isEmpty()]

        if not valid_dataframes:
            logging.log_info({
                "module": "APIService._validate_dataframes",
                "log_output_msg": "All DataFrames are empty",
                "status": "Skipped",
                "@LEVEL": "INFO",
            })
            return None

        return valid_dataframes


    @staticmethod
    def _create_combined_schema(processed_dataframes):
        """
        Creates a combined schema based on all DataFrames.

        Args:
            processed_dataframes (list): List of processed DataFrames

        Returns:
            StructType: Combined schema
        """
        # Collect all columns
        all_columns = set()
        for df in processed_dataframes:
            all_columns.update(df.columns)

        # Create combined schema
        combined_schema_fields = []
        for column in all_columns:
            # Find the most complex data type for each column
            data_types = []
            for df in processed_dataframes:
                if column in df.columns:
                    data_types.append(df.schema[column].dataType)

            # Prioritize more complex types
            if data_types:
                combined_schema_fields.append(StructField(column, data_types[0], True))

        return StructType(combined_schema_fields)

    @staticmethod
    def _align_and_combine_dataframes(processed_dataframes, parameters):
        """
        Aligns the schemas of the DataFrames and combines them into a single one,
        ensuring that columns with the same name have compatible types by forcing string conversion
        for all incompatible columns.
        Args:
            processed_dataframes (list): List of processed DataFrames
            parameters (dict): Processing parameters including cast_array_fields
        Returns:
            DataFrame: Combined DataFrame with harmonized column types
        """
        logging = AWSLogService()
        if not processed_dataframes:
            logging.log_error({
                "module": "APIService._align_and_combine_dataframes",
                "log_output_msg": "No DataFrames to combine",
                "status": "Error",
                "@LEVEL": "ERROR",
            })
            raise ValueError("No DataFrames to combine")
        if len(processed_dataframes) == 1 and parameters.get("cast_array_fields") is None:
            # Si solo hay un DataFrame, no hay nada que combinar
            return processed_dataframes[0]
        if len(processed_dataframes) == 1 and parameters.get("cast_array_fields"):
            string_df = processed_dataframes[0]
            for col_name in processed_dataframes[0].dtypes:
                if "array" not in col_name[1].lower():
                    string_df = string_df.withColumn(col_name[0], F.col(col_name[0]).cast("string"))
                elif "array<struct<" in col_name[1].lower()[:13]:
                    string_df = string_df.withColumn(col_name[0], F.expr(f"concat('[', array_join(transform({col_name[0]}, x -> to_json(x)), ','), ']')"))
                elif "array<" in col_name[1].lower()[:6]:
                    string_df = string_df.withColumn(col_name[0], F.col(col_name[0]).cast(ArrayType(StringType())))
            return string_df
        try:
            # SOLUCIÓN: Convertir todos los campos a string en cada DataFrame para eliminar
            # problemas de incompatibilidad de tipos
            string_dfs = []
            for i, df in enumerate(processed_dataframes):
                logging.log_info({
                    "module": "APIService._align_and_combine_dataframes",
                    "log_output_msg": f"Converting DataFrame {i + 1}/{len(processed_dataframes)} to string types",
                    "status": "Running",
                    "@LEVEL": "INFO",
                })
                # Convertir todos los campos a string
                string_df = df
                for col_name in df.dtypes:
                    if "array" not in col_name[1].lower():
                        string_df = string_df.withColumn(col_name[0], F.col(col_name[0]).cast("string"))
                    elif "array<struct<" in col_name[1].lower()[:13] and parameters.get("cast_array_fields"):
                        string_df = string_df.withColumn(col_name[0], F.expr(f"concat('[', array_join(transform({col_name[0]}, x -> to_json(x)), ','), ']')"))
                    elif "array<" in col_name[1].lower()[:6] and parameters.get("cast_array_fields"):
                        string_df = string_df.withColumn(col_name[0], F.col(col_name[0]).cast(ArrayType(StringType())))
                string_dfs.append(string_df)
            # Recolectar todos los nombres de columnas
            all_columns = set()
            for df in string_dfs:
                all_columns.update(df.columns)
            # Alinear DataFrames con todas las columnas
            aligned_dfs = []
            for i, df in enumerate(string_dfs):
                missing_columns = all_columns - set(df.columns)
                # Añadir columnas faltantes como null
                for col_name in missing_columns:
                    df = df.withColumn(col_name, F.lit(None).cast("string"))
                aligned_dfs.append(df)
            # Combinar todos los DataFrames alineados
            combined_df = aligned_dfs[0]
            for df in aligned_dfs[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            logging.log_info({
                "module": "APIService._align_and_combine_dataframes",
                "log_output_msg": f"Successfully combined {len(processed_dataframes)} DataFrames with string types",
                "status": "Completed",
                "@LEVEL": "INFO",
            })
            return combined_df
        except Exception as e:
            logging.log_error({
                "module": "APIService._align_and_combine_dataframes",
                "log_output_msg": f"Error combining DataFrames: {str(e)}",
                "status": "Error",
                "@LEVEL": "ERROR",
            })
            raise ValueError(f"Failed to combine DataFrames: {str(e)}") from e

    @staticmethod
    def _write_to_storage(df, s3_path, file_type, write_mode, partition_columns, table_name, extraction_folder, logging):
        """
        Writes the DataFrame to the specified S3 storage location, organizing it within a
        configurable extraction subfolder and handling path formatting.
        This method logs the operation progress and uses SparkService to save the data
        with the provided format, write mode, and partition settings.

        Parameters:
            df : DataFrame
                DataFrame to be saved.
            s3_path : str
                Base S3 path where data will be stored.
            file_type : str
                Format for saving the data (parquet, csv, delta).
            write_mode : str
                Spark write mode (overwrite, append, error, ignore).
            partition_columns : list
                Columns to partition the data.
            table_name : str
                Name of the table being saved (for logging only).
            extraction_folder : str
                Name of the subfolder where data will be saved. Default is "extraction".
            logging : object
                Logging object with log_info method.
        """
        # Ensure the path ends with /
        s3_path = s3_path.rstrip("/")
        # Use the extraction_folder parameter with a default fallback
        extraction_folder = extraction_folder or "extraction"
        # Add the extraction subfolder to the path
        extraction_path = f"{s3_path}/{extraction_folder}/"
        logging.log_info({
            "module": "APIService._write_to_storage",
            "log_output_msg": f"Saving data to {extraction_folder} folder: {extraction_path}",
            "status": "Running",
            "@LEVEL": "DEBUG",
        })
        SparkService.save_frame(
            df,
            extraction_path,
            file_type,
            write_mode,
            partition_columns
        )
        logging.log_info({
            "module": "APIService._write_to_storage",
            "log_output_msg": f"**** The table {table_name} was saved in s3 {extraction_folder} folder ****",
            "status": "Completed",
            "@LEVEL": "DEBUG",
        })

    _api_responses = []

    @classmethod
    def store_api_response(cls, response):
        """
        Stores an API response for later processing.

        Args:
            response (dict): API response to store
        """
        if response is not None:
            cls._api_responses.append(response)

    @classmethod
    def save_combined_json(cls, bucket_name, prefix_path, process_name, decompressed_folder="decompressed"):
        """
         Caches an API response in the class's internal collection for subsequent batch processing.
        This method adds valid API responses to a class-level collection, allowing the application
        to gather multiple responses before processing them together. The response is only stored
        if it contains valid data (not None).
        Parameters:
        -----------
        response : dict
            The API response dictionary to be stored. This should contain all relevant data
            returned from an API call, including status information, headers, and the response body.
            If the response is None, it will be ignored and not added to the collection.
        Returns:
        --------
        None
            This method doesn't return any value; it only performs the storage operation.
        """
        if not cls._api_responses:
            return
        try:
            logging = AWSLogService()
            # Create a combined object with all responses
            combined_data = {
                "process_name": process_name,
                "created_at": datetime.now().isoformat(),
                "responses": cls._api_responses
            }
            # Convert to JSON
            json_data = json.dumps(combined_data, default=str)
            # Create a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            prefix_path = prefix_path.strip("/") if prefix_path else ""
            file_key = f"{prefix_path}/{decompressed_folder}/combined_responses_{process_name}_{timestamp}.json"
            logging.log_info({
                "module": "APIService.save_combined_json",
                "log_output_msg": f"Saving combined API responses to {decompressed_folder} folder: {file_key}",
                "status": "Running",
                "@LEVEL": "DEBUG",
            })
            # Upload to S3
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Body=json_data,
                Bucket=bucket_name,
                Key=file_key
            )
            # Clear stored responses
            cls._api_responses = []

            logging.log_info({
                "module": "APIService.save_combined_json",
                "log_output_msg": f"Successfully saved combined API responses to {decompressed_folder} folder",
                "status": "Completed",
                "@LEVEL": "DEBUG",
            })

        except (BotoCoreError, ClientError, IOError, ValueError, TypeError) as e:
            logging = AWSLogService()
            logging.log_error({
                "module": "APIService.save_combined_json",
                "log_output_msg": f"Error saving combined JSON to {decompressed_folder} folder: {str(e)}",
                "status": "Error",
                "@LEVEL": "WARNING",
            })

    @staticmethod
    def _create_mixed_type_schema(dict_array):
        """
        Creates a schema that can handle mixed types in an array of dictionaries.
        For fields with mixed types, StringType is used.
        """
        if not dict_array or not isinstance(dict_array[0], dict):
            return StructType([])
        # Recopilar todos los campos y sus tipos
        field_types = {}
        for item in dict_array:
            for field, value in item.items():
                if field not in field_types:
                    field_types[field] = set()
                # Registrar el tipo (omitir None)
                if value is not None:
                    if isinstance(value, (int, float, bool, str)):
                        field_types[field].add(type(value).__name__)
                    else:
                        field_types[field].add("complex")
        # Crear los campos del schema
        fields = []
        for field, types in field_types.items():
            # Si hay múltiples tipos, usar StringType
            if len(types) > 1:
                fields.append(StructField(field, StringType(), True))
            else:
                # Para un solo tipo, inferir normalmente desde el primer elemento con ese campo
                for item in dict_array:
                    if field in item and item[field] is not None:
                        field_type = APIService.infer_field_type(item[field])
                        fields.append(StructField(field, field_type, True))
                        break
                else:
                    # Si todos los valores son None, usar StringType
                    fields.append(StructField(field, StringType(), True))
        return StructType(fields)

    @staticmethod
    def standardize_mixed_types(json_data):
        """
        Pre-procesa datos JSON para estandarizar tipos mixtos dentro de arrays y estructuras anidadas.
        Cuando se detectan tipos mixtos en un mismo campo o en elementos de un array,
        convierte todos los valores a strings para evitar problemas con el schema.
        Args:
            json_data (list): Lista de registros JSON a procesar
        Returns:
            list: Lista de registros con tipos mixtos estandarizados
        """
        if not json_data:
            return json_data
        # Detectar campos con tipos mixtos a través de múltiples registros
        field_types = {}
        for record in json_data:
            if not isinstance(record, dict):
                continue
            for key, value in record.items():
                if key not in field_types:
                    field_types[key] = set()
                # Capturar tipo básico
                if value is None:
                    field_types[key].add("null")
                elif isinstance(value, bool):
                    field_types[key].add("boolean")
                elif isinstance(value, int):
                    field_types[key].add("integer")
                elif isinstance(value, float):
                    field_types[key].add("float")
                elif isinstance(value, str):
                    field_types[key].add("string")
                elif isinstance(value, dict):
                    field_types[key].add("struct")
                elif isinstance(value, list):
                    field_types[key].add("array")
                    # Para arrays, analizar tipos internos
                    if value and key + "_element_types" not in field_types:
                        field_types[key + "_element_types"] = set()
                    # Detectar tipos mixtos dentro del array
                    if value:
                        for item in value:
                            if item is None:
                                continue
                            # Para arrays de objetos, verificar tipos de campos comunes
                            if isinstance(item, dict) and "id" in item:
                                if key + "_id_types" not in field_types:
                                    field_types[key + "_id_types"] = set()
                                if item["id"] is None:
                                    field_types[key + "_id_types"].add("null")
                                elif isinstance(item["id"], bool):
                                    field_types[key + "_id_types"].add("boolean")
                                elif isinstance(item["id"], int):
                                    field_types[key + "_id_types"].add("integer")
                                elif isinstance(item["id"], float):
                                    field_types[key + "_id_types"].add("float")
                                elif isinstance(item["id"], str):
                                    field_types[key + "_id_types"].add("string")
                            # Capturar tipo del elemento
                            elif isinstance(item, bool):
                                field_types[key + "_element_types"].add("boolean")
                            elif isinstance(item, int):
                                field_types[key + "_element_types"].add("integer")
                            elif isinstance(item, float):
                                field_types[key + "_element_types"].add("float")
                            elif isinstance(item, str):
                                field_types[key + "_element_types"].add("string")
                            elif isinstance(item, dict):
                                field_types[key + "_element_types"].add("struct")
                            elif isinstance(item, list):
                                field_types[key + "_element_types"].add("array")
        # Identificar campos que necesitan estandarización
        mixed_type_fields = {
            k: v for k, v in field_types.items()
            if len(v - {"null"}) > 1 and "null" not in v  # Tipos mixtos no nulos
            or (len(v) > 1 and "null" in v and len(v) > 2)  # Más de un tipo no nulo
            or k.endswith("_id_types") and len(v) > 1  # Tipos mixtos en campos id
            or k.endswith("_element_types") and len(v) > 1  # Tipos mixtos en arrays
        }
        # Si no hay tipos mixtos, devolver los datos originales
        if not mixed_type_fields:
            return json_data
        # Si hay tipos mixtos, procesarlos
        result = []
        for record in json_data:
            if not isinstance(record, dict):
                result.append(record)
                continue
            processed_record = {}
            for key, value in record.items():
                # Si el campo tiene tipos mixtos o es un array con elementos de tipos mixtos
                if key in mixed_type_fields or key + "_element_types" in mixed_type_fields:
                    if isinstance(value, list):
                        # Procesar array con elementos de tipos mixtos
                        processed_value = []
                        for item in value:
                            if isinstance(item, dict) and key + "_id_types" in mixed_type_fields:
                                # Convertir campos id a string en objetos dentro de arrays
                                processed_item = {}
                                for item_key, item_value in item.items():
                                    if item_key == "id" and item_value is not None:
                                        processed_item[item_key] = str(item_value)
                                    else:
                                        processed_item[item_key] = item_value
                                processed_value.append(processed_item)
                            elif item is not None and not isinstance(item, (dict, list)):
                                # Convertir elementos simples a string
                                processed_value.append(str(item))
                            else:
                                processed_value.append(item)
                        processed_record[key] = processed_value
                    elif value is not None and not isinstance(value, (dict, list)):
                        # Convertir valores simples a string
                        processed_record[key] = str(value)
                    else:
                        processed_record[key] = value
                else:
                    processed_record[key] = value
            result.append(processed_record)
        return result

    @staticmethod
    def ensure_consistent_types(json_data):
        """
        Ensure all values in the JSON have consistent types, converting as needed.
        This is a simpler and more aggressive approach than the previous implementation,
        ensuring that types are consistent throughout the data.
        Args:
            json_data (list): List of dictionaries to process
        Returns:
            list: List with consistent types
        """
        if not json_data:
            return json_data
        # Función recursiva para procesar objetos JSON

        def process_value(value):
            if value is None:
                return None
            if isinstance(value, bool):  # Cambiado de elif a if
                return value
            if isinstance(value, (int, float)):  # Cambiado de elif a if
                return value
            if isinstance(value, str):  # Cambiado de elif a if
                # Verificar si el string representa un booleano
                if value.lower() in ('true', 'false'):
                    return value  # Mantener como string para evitar conflictos
                # Para otros tipos de strings, mantenerlos como están
                return value
            if isinstance(value, dict):  # Cambiado de elif a if
                return {k: process_value(v) for k, v in value.items()}
            if isinstance(value, list):  # Cambiado de elif a if
                # Para listas, procesar cada elemento
                return [process_value(item) for item in value]
            # Para cualquier otro tipo, convertir a string (no se necesita else aquí)
            return str(value)
        # Procesar cada registro
        return [process_value(record) for record in json_data]

    @staticmethod
    def convert_problematic_types_to_string(json_data):
        """
        Convierte todos los valores de tipos que puedan causar problemas a strings,
        manteniendo solo los tipos básicos como strings, enteros, y booleanos.
        Args:
            json_data (list): Lista de diccionarios a procesar
        Returns:
            list: Lista con tipos de datos seguros
        """
        def convert_node(node):
            if node is None:
                return None
            if isinstance(node, bool):
                return node
            if isinstance(node, (int, float)):
                return node
            if isinstance(node, str):
                return node
            if isinstance(node, dict):
                return {k: convert_node(v) for k, v in node.items()}
            if isinstance(node, list):
                if all(isinstance(item, dict) for item in node if item is not None):
                    return [convert_node(item) for item in node]
                return [
                    item if isinstance(item, (str, int, float, bool)) or item is None
                    else str(item) for item in node
                ]
            return str(node)
        return [convert_node(record) for record in json_data]


    @staticmethod
    def convert_complex_types_to_string(data):
        """
        Convierte estructuras complejas anidadas a strings para facilitar la creación del DataFrame.
        Args:
            data (list): Lista de registros a procesar
        Returns:
            list: Lista con valores complejos convertidos a strings
        """
        def process_value(val):
            if val is None:
                return None
            if isinstance(val, (bool, int, float, str)):
                return val
            if isinstance(val, (dict, list)):
                try:
                    return json.dumps(val)
                except (TypeError, OverflowError):
                    return str(val)
            return str(val)
        result = []
        for item in data:
            if isinstance(item, dict):
                processed_item = {}
                for key, value in item.items():
                    processed_item[key] = process_value(value)
                result.append(processed_item)
            else:
                try:
                    result.append({"value": process_value(item)})
                except (TypeError, ValueError, AttributeError):
                    pass
        return result

    @staticmethod
    def _align_dataframes_schema(dataframes: List[DataFrame]) -> List[DataFrame]:
        """
        Aligns the schema of multiple DataFrames based on the first one.
        Args:
            dataframes: List of DataFrames to align.
        Returns:
            List of DataFrames with aligned schemas, including the reference DataFrame.
        Raises:
            ValueError: If the list is empty.
        """
        logging = AWSLogService()
        logging.log_info({
            "module": "APIService.align_dataframes_schema",
            "log_output_msg": "Starts process to Aligns the schema of multiple DataFrames",
            "status": "Start",
            "@LEVEL": "DEBUG",
        })
        if not dataframes:
            raise ValueError("La lista de DataFrames no puede estar vacía")
        if len(dataframes) == 1:
            logging.log_info({
                "module": "APIService.align_dataframes_schema",
                "log_output_msg": "This list only have one DataFrame",
                "status": "END",
                "@LEVEL": "DEBUG",
            })
            return dataframes
        # Obtener el esquema de referencia del primer DataFrame
        reference_df = dataframes[0]
        reference_columns = reference_df.columns
        aligned_dfs = [reference_df]  # Incluir el DataFrame de referencia en la lista resultante
        logging.log_info({
            "module": "APIService.align_dataframes_schema",
            "log_output_msg": "Schema of the first DataFrame",
            "status": "Start",
            "@LEVEL": "DEBUG",
        })
        reference_df.printSchema()
        # Alinear el resto de DataFrames al esquema de referencia
        for df in dataframes[1:]:
            # Identificar columnas faltantes
            missing_columns = set(reference_columns) - set(df.columns)
            if not missing_columns and df.columns == reference_columns:
                logging.log_info({
                    "module": "APIService.align_dataframes_schema",
                    "log_output_msg": "The current DataFrame have the same schema",
                    "status": "Start",
                    "@LEVEL": "DEBUG",
                })
                aligned_dfs.append(df)
            else:
                logging.log_info({
                    "module": "APIService.align_dataframes_schema",
                    "log_output_msg": f"Missing columns in the current DataFrame: {missing_columns}",
                    "status": "Start",
                    "@LEVEL": "DEBUG",
                })
                # Añadir columnas faltantes con valores nulos
                aligned_df = df
                for column in missing_columns:
                    # Obtener el tipo de datos de la columna del esquema de referencia
                    col_type = reference_df.schema[column].dataType
                    logging.log_info({
                        "module": "APIService.align_dataframes_schema",
                        "log_output_msg": f"Adding the column in the current DataFrame: {column} - The datatype it's:{col_type}",
                        "status": "Start",
                        "@LEVEL": "DEBUG",
                    })
                    aligned_df = aligned_df.withColumn(column, F.lit(None).cast(col_type))
                # Seleccionar columnas en el mismo orden que el DataFrame de referencia
                aligned_df = aligned_df.select(reference_columns)
                logging.log_info({
                    "module": "APIService.align_dataframes_schema",
                    "log_output_msg": "The New Schema and orden of the dataFrame is: ",
                    "status": "Start",
                    "@LEVEL": "DEBUG",
                })
                aligned_df.printSchema()
                aligned_dfs.append(aligned_df)
        logging.log_info({
            "module": "APIService.align_dataframes_schema",
            "log_output_msg": "the process has been completed",
            "status": "END",
            "@LEVEL": "DEBUG",
        })
        return aligned_dfs
