# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        hudi_connector
Descripcion: 	Utileria para conectarse a base de datos utilizando hudi.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
from pyspark.sql.readwriter import DataFrameReader
from interfaces.compaction import CompactionInterface
from services.spark_service import SparkService, SparkServiceException
from services.iceberg_service import SparkIcebergService, SparkIcebergServiceException


class IcebergConnectorException(Exception):
    pass


class IcebergConnector(CompactionInterface):
    @staticmethod
    def read_data(spark, file_path: str, data_format: str, parameters: dict):
        """
        Pull the data from the flat file stored.
        Args:
            parameters (dict): The parameters to connect to the database.
        Raises:
            JdbcConnectorException: An exception if an error occurred.
        Returns:
            DataFrameReader: The data in format of dataframe.
        """
        try:
            return SparkService.read_from_file(
                spark, file_path, data_format, parameters
            )
        except SparkServiceException as spark_error:
            raise SparkIcebergServiceException(spark_error) from spark_error

    @staticmethod
    def insert_data(spark, dataframe: DataFrameReader, parameters: dict):
        """
        Saves the data in the location and format defined.
        Args:
            dataframe (DataFrameReader): The frame with the data.
            location (str): The location where the data will be stored.
            data_format (str): The format of how the data will be stored.
            mode (str): Overwrite or append.
        Raises:
            JdbcConnectorException: An exception if an error occurred.
        """
        try:
            SparkIcebergService.table_exists(spark, dataframe, parameters)
            if parameters.get('add_input_missing_columns'):
                df = SparkIcebergService.add_missing_cols_input(spark, dataframe, parameters)
                SparkIcebergService.upsert_data(spark, df, parameters)
            else:
                SparkIcebergService.upsert_data(spark, dataframe, parameters)
        except SparkServiceException as spark_error:
            raise SparkIcebergServiceException(spark_error) from spark_error
