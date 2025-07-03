# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Adolfo Benitez Herrera
Proyecto:       Datalake Back
Proceso:        data_lifecycle_connector
Descripcion: 	Utileria para archivar y eliminar datos de una tabla que excedan cierta antiguedad.
Modificacion:
                2025-03-26 - Creación
===================================================================================================
"""

from interfaces.data_lifecycle import DataLifecycleInterface
from services.data_lifecycle_service import (
    DataLifecycleService,
    DataLifecycleServiceException,
)


class IcebergConnectorException(Exception):
    pass


class DataLifecycleConnector(DataLifecycleInterface):
    @staticmethod
    def drop_and_create_archive_table(spark, query_drop_table, create_table_query):
        """
        Drops and create a new iceberg wrk table for archiving.
        Args:
            parameters (dict): The parameters to perform the data lifecycle.
        Raises:
            DataLifecycleServiceException: An exception if an error occurred.
        """
        try:
            DataLifecycleService.drop_archive_table(spark, query_drop_table)
            return DataLifecycleService.create_archive_table(spark, create_table_query)

        except DataLifecycleServiceException as spark_error:
            raise DataLifecycleServiceException(spark_error) from spark_error

    @staticmethod
    def archive_data(spark, sql_file):
        """
        Deletes the old data in the current table using transformations.
        Args:
            parameters (dict): The parameters to perform the data lifecycle.
        Raises:
            DataLifecycleServiceException: An exception if an error occurred.
        """
        try:
            return DataLifecycleService.archive_data_with_sql(spark, sql_file)
        except DataLifecycleServiceException as spark_error:
            raise DataLifecycleServiceException(spark_error) from spark_error
