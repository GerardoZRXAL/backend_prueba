# -*- coding: utf-8 -*-
"""
===================================================================================================
Author:         Adolfo Benitez Herrera
Project:        Datalake Back
Process:        api_connector
Description: 	Utility to get data from an API.
Modification:
               2024-12-11 - Creation
               2025-03-07 - Refactoring: simplification of responsibilities
===================================================================================================
"""

from interfaces.api import APIInterface
from services.api_service import APIService, APIServiceException
from loggers.aws_log_service import AWSLogService

class APIConnectionError(Exception):
    """Custom exception for API connection errors"""


class APIConnector(APIInterface):
    @staticmethod
    def process_api_data(spark, api_config: dict, pagination_type):
        """
        Process API data using the provided configuration.
        Args:
            spark: Spark session
            api_config (dict): Configuration dictionary containing API details
            pagination_type (str): Type of pagination ('page' or 'cursor')
        Returns:
            For pagination_type='page': tuple(DataFrame, has_next_page)
            For pagination_type='cursor': tuple(DataFrame, has_more, after_token)
        Raises:
            APIServiceException: If any error occurs during processing
        """
        try:
            response = APIService.get_api_data(api_config, pagination_type)

            # Pass the response to APIService for storage
            if response is not None:
                APIService.store_api_response(response)

            if response is None:
                if pagination_type == "page":
                    return None, False
                if pagination_type == "cursor":
                    return None, False, None
                return None, False

            body_name = api_config.get("body_name", None)
            if pagination_type == "page":
                df, next_page = APIService.create_dataframe_from_response(
                    spark, response, body_name, pagination_type
                )
                return df, bool(next_page)
            if pagination_type == "cursor":
                df, has_more, after_token = APIService.create_dataframe_from_response(
                    spark, response, body_name, pagination_type
                )
                return df, has_more, after_token

            # Default return for any other pagination type
            df, next_page = APIService.create_dataframe_from_response(
                spark, response, body_name, "page"
            )
            return df, bool(next_page)
        except APIServiceException as spark_error:
            raise APIServiceException(spark_error) from spark_error

    @staticmethod
    def insert_data(dataframe, parameters):
        """
        Saves the data in the s3 location defined with the specified format.
        Also saves all API responses in a single JSON file.
        Verifies if the table exists in the catalog and creates it if needed.
        Args:
            dataframes (list): List of DataFrames to save
            parameters (dict): Parameters for saving the data, must include:
                - bucket_name: S3 bucket name
                - prefix_path: Prefix path in the bucket
                - table_name: Name of the table/process
                - file_type: Format to save in (parquet, csv, etc.)
                - mode (optional): Write mode (append, overwrite, etc.)
                - partition_by (optional): Partition columns
                - extraction_folder (optional): Folder for data extraction (default: 'extraction')
                - decompressed_folder (optional): Folder for saving raw API responses (default: 'decompressed')
                - database_name (optional): Name of the database in the catalog
        Raises:
            APIServiceException: If any error occurs during saving
        """
        try:
            # Inicializar el servicio de logging
            log_utils = AWSLogService()
            # Extract necessary parameters
            bucket_name = parameters.get("bucket_name")
            prefix_path = parameters.get("prefix_path", "")
            table_name = parameters.get("table_name")
            database_name = parameters.get("database_name", "default")
            # Construct the s3_path and add it to parameters if it doesn't exist
            if "s3_path" not in parameters or parameters.get("s3_path") is None:
                s3_path = f"s3://{bucket_name}/{prefix_path}"
                parameters["s3_path"] = s3_path
            # Create updated parameters that include bucket_name and prefix_path
            updated_parameters = parameters.copy()
            # Check if running in a Spark environment with catalog access
            if hasattr(dataframe, 'sparkSession') and dataframe.sparkSession is not None:
                spark = dataframe.sparkSession
                # Verify if the table exists in the catalog
                table_exists = False
                try:
                    # Using SparkSession's catalog to check if table exists
                    # Usar métodos públicos en lugar de acceder a miembros protegidos
                    table_exists = bool(spark.catalog.tableExists(f"{database_name}.{table_name}"))
                except (AttributeError, TypeError) as e:
                    log_utils.log_info({
                        "module": "APIConnector.insert_data",
                        "log_output_msg": f"Could not check if table exists: {str(e)}",
                        "@LEVEL": "WARNING"
                    })
                # If the table doesn't exist, create it
                if not table_exists:
                    log_utils.log_info({
                        "module": "APIConnector.insert_data",
                        "log_output_msg": f"Table {database_name}.{table_name} does not exist. Creating it.",
                        "@LEVEL": "INFO"
                    })
                    try:
                        # Save the dataframe as a new table in the catalog
                        partition_cols = parameters.get("partition_by", [])
                        writer = dataframe.write.format(parameters.get("file_type", "parquet"))
                        # Apply partitioning if specified
                        if partition_cols:
                            writer = writer.partitionBy(*partition_cols)
                        # Set any additional write options
                        for key, value in parameters.get("write_options", {}).items():
                            writer = writer.option(key, value)
                        # Create the table
                        writer.mode("overwrite") \
                            .option("path", parameters["s3_path"]) \
                            .saveAsTable(f"{database_name}.{table_name}")
                        log_utils.log_info({
                            "module": "APIConnector.insert_data",
                            "log_output_msg": f"Successfully created table {database_name}.{table_name}",
                            "@LEVEL": "INFO"
                        })
                    except (ValueError, IOError, RuntimeError) as create_error:
                        log_utils.log_error({
                            "module": "APIConnector.insert_data",
                            "log_output_msg": f"Failed to create table {database_name}.{table_name}: {str(create_error)}",
                            "@LEVEL": "ERROR"
                        })
                        # Continue with normal S3 write even if table creation fails
            # Delegate all saving logic to APIService
            APIService.write_data(dataframe, updated_parameters)
            # Also save API responses as JSON
            decompressed_folder = parameters.get("decompressed_folder", "decompressed")
            APIService.save_combined_json(
                bucket_name,
                prefix_path,
                table_name,
                decompressed_folder
            )
        except APIServiceException as spark_error:
            raise APIServiceException(spark_error) from spark_error
        except Exception as e:
            raise APIServiceException(f"Error in insert_data: {str(e)}") from e
