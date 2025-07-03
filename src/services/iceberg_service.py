# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        services / iceberg_services
Descripcion: 	Utileria para realizar las operaciones con iceberg.
Modificacion:
                2023-10-05 - Creación
===================================================================================================
"""
from pyspark.sql.functions import max, col
import constants.aws_constants as AwsConstants
from services.spark_service import SparkService, SparkServiceException
from loggers.aws_log_service import AWSLogService
from utils.common_utils import CommonUtils


class SparkIcebergServiceException(Exception):
    pass


class SparkIcebergService:
    @staticmethod
    def upsert_data(spark, data_frame, parameters):
        """
        This function updates the data or inserts new records
        Args:
            data_frame (dataframe): The dataframe that will be inserted
            glue_catalog (str) : AWS Glue DataCatalog
            data_bases (string): Name of the database where the table is located
            table_name (string): Name of the table to be updated
            primary_keys (list): List of primary keys for updating table data
        Raises:
            SparkIcebergServiceException: If an error occurred this exception will be raised
        Returns:
            None
        """
        logging = AWSLogService()
        glue_catalog = parameters.get("glue_catalog")
        data_bases = parameters.get("data_bases")
        table_name = parameters.get("table_name")
        primary_keys = parameters.get("primary_keys")
        precombine_field = parameters.get("precombine_field")
        apply_window_func_bf_write = parameters.get("apply_window_func_bf_write")
        try:
            if len(primary_keys) > 1:
                conditions = " AND ".join(f"t.{key} = s.{key}" for key in primary_keys)
            else:
                conditions = f"t.{primary_keys[0]} = s.{primary_keys[0]}"
            if precombine_field is not None:
                precombine_field = [field.strip() for field in precombine_field.split(",")]
                # df_max = data_frame.groupBy(primary_keys).agg(
                #     max(precombine_field).alias(precombine_field)
                # )
                df_max = data_frame.groupBy(primary_keys).agg(
                    *[max(col(pre_key)).alias(pre_key) for pre_key in precombine_field]
                )
                # data_frame = data_frame.join(
                #     df_max, primary_keys + [precombine_field], "INNER"
                # ).select(data_frame["*"])
                data_frame = data_frame.join(
                    df_max, primary_keys + precombine_field, "INNER"
                ).select(data_frame["*"])
                data_frame = data_frame.distinct()
            if apply_window_func_bf_write:
                data_frame = SparkService.apply_window_func_to_df(
                    data_frame, apply_window_func_bf_write.get('primary_keys'), apply_window_func_bf_write.get('order_by')
                )
            data_frame.createOrReplaceTempView(f"{table_name}_raw_data")
            if parameters.get("type_of_load") == "append":
                query_upsert = AwsConstants.QUERY_APPEND.format(
                    glue_catalog, data_bases, table_name
                )
            else:
                query_upsert = AwsConstants.QUERY_UPSERT.format(
                    glue_catalog, data_bases, table_name, conditions
                )
            df = SparkService.read_data_query(spark, query_upsert)
            logging.log_info(
                {
                    "module": "SparkIcebergService.upsert_data",
                    "log_output_msg": f"**** The table {table_name} was saved in format iceberg ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            return df
        except SparkServiceException as error:
            logging.log_error(
                {
                    "module": "SparkIcebergService.upsert_data",
                    "log_output_msg": (
                        f"***** Error saving the table {table_name}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkIcebergServiceException(error) from error

    @staticmethod
    def table_exists(spark, data_frame, parameters) -> bool:
        """
        This function validates if the table to which the operation will be applied exists.
        If it exists, it updates the table. If it does not exist, it creates and updates it.
        Args:
            data_frame (dataframe): The dataframe that will be inserted
            glue_catalog (str) : AWS Glue DataCatalog
            s3_bucket (str): S3 path where the table will be created if it does not exist
            data_bases (str): Database name to search table
            table_name (str): Name of the table being validated
        Raises: SparkIcebergServiceException
        Returns:
            table_exists (boolean): Table existence variable
        """
        glue_catalog = parameters.get("glue_catalog")
        s3_bucket = parameters.get("s3_bucket")
        data_bases = parameters.get("data_bases")
        table_name = parameters.get("table_name")
        partition_columns = parameters.get("partition_columns")
        table_exists = False
        query_exist = AwsConstants.QUERY_VALIDATE_TABLE.format(data_bases, table_name)
        result = SparkService.read_data_query(spark, query_exist)
        logging = AWSLogService()
        if result.count() == 0:
            table_exists = True
            schema_columns = data_frame.schema
            location = f"{s3_bucket}/tb_{table_name}"
            columns = (
                ",".join([field.simpleString() for field in schema_columns])
            ).replace(":", " ")
            if partition_columns is not None:
                create_table_query = AwsConstants.QUERY_CREATE_TABLE_ICEBERG_PARTITION.format(
                    glue_catalog, data_bases, table_name, columns, location, partition_columns
                )
            else:
                create_table_query = AwsConstants.QUERY_CREATE_TABLE_ICEBERG.format(
                    glue_catalog, data_bases, table_name, columns, location
                )
            SparkService.read_data_query(spark, create_table_query)
            logging.log_info(
                {
                    "module": "SparkIcebergService.table_exists",
                    "log_output_msg": f"**** The table {table_name} is going to be created ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            logging.log_info(
                {
                    "module": "SparkIcebergService.table_exists",
                    "log_output_msg": f"**** The table {table_name} was created ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
        else:
            logging.log_info(
                {
                    "module": "SparkIcebergService.table_exists",
                    "log_output_msg": f"**** The table {table_name} is already created ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            logging.log_info(
                {
                    "module": "SparkIcebergService.table_exists",
                    "log_output_msg": "**** Start process to validate table schema vs incoming data schema ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            get_data_query = AwsConstants.QUERY_GET_TABLE_DATA.format(
                glue_catalog, data_bases, table_name
            )
            df_apache_iceberg_table = SparkService.read_data_query(spark, get_data_query)
            parquet_schema = data_frame.schema
            iceberg_schema = df_apache_iceberg_table.schema
            new_columns = []
            for field in parquet_schema.fields:
                if field.name not in iceberg_schema.names:
                    logging.log_info(
                        {
                            "module": "SparkIcebergService.table_exists",
                            "log_output_msg": f"**** Column: {field.name} - data_type: {field.dataType} ****",
                            "status": "Running",
                            "@LEVEL": "DEBUG",
                        }
                    )
                    new_columns.append(f"{field.name} {CommonUtils.get_spark_sql_type(field.dataType)}")
            if new_columns:
                new_columns_str = {', '.join(new_columns)}
                alter_query = AwsConstants.QUERY_ADD_COLUMN.format(
                    glue_catalog, data_bases, table_name, list(new_columns_str)[0]
                )
                SparkService.read_data_query(spark, alter_query)
                logging.log_info(
                    {
                        "module": "SparkIcebergService.table_exists",
                        "log_output_msg": f"**** New columns was added to the table {table_name} ****",
                        "status": "Running",
                        "@LEVEL": "DEBUG",
                    }
                )
            else:
                logging.log_info(
                    {
                        "module": "SparkIcebergService.table_exists",
                        "log_output_msg": "**** No new columns found - Continue process ****",
                        "status": "Running",
                        "@LEVEL": "DEBUG",
                    }
                )
        return table_exists

    @staticmethod
    def add_missing_cols_input(spark, data_frame, parameters):
        """
        This function adds missing columns from the table schema to the input DataFrame
        Args:
            data_frame (dataframe): The dataframe that will be processed
            glue_catalog (str) : AWS Glue DataCatalog
            data_bases (string): Name of the database where the table is located
            table_name (string): Name of the table to reference for schema
        Raises:
            SparkIcebergServiceException: If an error occurred this exception will be raised
        Returns:
            DataFrame: Processed DataFrame with missing columns added
        """
        logging = AWSLogService()
        glue_catalog = parameters.get("glue_catalog")
        data_bases = parameters.get("data_bases")
        table_name = parameters.get("table_name")
        try:
            logging.log_info({
                "module": "SparkIcebergService.add_missing_cols_input",
                "log_output_msg": "**** Initiates the method for add missing cols ****",
                "status": "Running",
                "@LEVEL": "DEBUG",
            })
            # Verify if the table exists first
            query_exist = AwsConstants.QUERY_VALIDATE_TABLE.format(data_bases, table_name)
            result = SparkService.read_data_query(spark, query_exist)
            if result.count() == 0:
                logging.log_info({
                    "module": "SparkIcebergService.add_missing_cols_input",
                    "log_output_msg": f"**** Table {data_bases}.{table_name} does not exist. Cannot add missing columns. ****",
                    "status": "Skipped",
                    "@LEVEL": "INFO",
                })
                # Just return the original DataFrame since we can't match schema with non-existent table
                return data_frame
            # If we reached here, the table exists and we can proceed
            get_data_query = AwsConstants.QUERY_GET_TABLE_DATA.format(
                glue_catalog, data_bases, table_name
            )
            df_apache_iceberg_table = SparkService.read_data_query(spark, get_data_query)
            iceberg_schema = df_apache_iceberg_table.schema

            df = SparkService.add_missing_schema_columns(data_frame, iceberg_schema)
            df = SparkService.cast_columns_to_match_schema(df, iceberg_schema)
            logging.log_info({
                "module": "SparkIcebergService.add_missing_cols_input",
                "log_output_msg": "**** Completed method for adding missing cols ****",
                "status": "Running",
                "@LEVEL": "DEBUG",
            })
            return df
        except SparkServiceException as error:
            logging.log_error({
                "module": "SparkIcebergService.add_missing_cols_input",
                "log_output_msg": (
                    f"***** Error add missing cols for the table {table_name}: {error} *****"
                ),
                "status": "Error",
                "@LEVEL": "ERROR",
            })
            raise SparkIcebergServiceException(error) from error
