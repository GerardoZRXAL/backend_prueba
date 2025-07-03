# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        s3_connector
Descripcion: 	Utileria para ejecutar sentencias en spark.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
from pyspark.sql.readwriter import DataFrameReader
from interfaces.connector import ConnectorInterface
from interfaces.secret_manager import SecretInterface
from secrets_managers.aws_secret_service import AWSSecretServiceException
from services.spark_service import SparkService, SparkServiceException


class S3ConnectorException(Exception):
    pass


class S3Connector(ConnectorInterface):
    @staticmethod
    def get_credentials(
        secret_service: SecretInterface, secret_name: str, region_name: str
    ):
        """
        Retrieve the credentials to connect to the S3 server.
        Args:
            secret_service (SecretInterface): Secret service to get the credentials.
            secret_name (str): The secret's name.
            region_name (str): The region where the secret is stored.
        Raises:
            PostgresConnectorException: An exception if an error occurred.
        Returns:
            str: The secret's value.
        """
        try:
            return secret_service.get_secret(secret_name, region_name)
        except AWSSecretServiceException as secret_error:
            raise S3ConnectorException(secret_error) from secret_error

    @staticmethod
    def pull_data(spark, parameters: dict) -> bytes:
        """
        Pull the data from the file that is stored in the S3.
        Args:
            parameters (dict): The parameters to connect to the database.
        Raises:
            S3ConnectorException: An exception if an error occurred.
        Returns:
            bytes: The data in format of bytes.
        """
        try:
            return SparkService.read_from_file(
                spark,
                file_path=parameters.get("file_path"),
                data_format=parameters.get("data_format"),
                parameters=parameters,
            )
        except SparkServiceException as spark_error:
            raise S3ConnectorException(spark_error) from spark_error

    @staticmethod
    def save_data(
        dataframe: DataFrameReader, location: str, data_format: str, mode: str, partition_cols=None,
    ):
        """
        Saves the data in the location and format defined.
        Args:
            dataframe (DataFrameReader): The frame with the data.
            location (str): The location where the data will be stored.
            data_format (str): The format of how the data will be stored.
            mode (str): Overwrite or append.
            partition_cols (str, optional): The partition columns. Defaults to None.
        Raises:
            PostgresConnectorException: An exception if an error occurred.
        """
        try:
            SparkService.save_frame(dataframe, location, data_format, mode, partition_cols)
        except SparkServiceException as spark_error:
            raise S3ConnectorException(spark_error) from spark_error
