# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Adolfo Benitez Herrera
Proyecto:       Datalake Back
Proceso:        data_profiling_connector
Descripcion: 	Utileria para obtener el perfil de datos de una tabla.
Modificacion:
                2025-03-18 - Creación
===================================================================================================
"""

from pyspark.sql.readwriter import DataFrameReader
from interfaces.data_profiling import DataProfilingInterface
from services.spark_service import SparkService, SparkServiceException
from services.iceberg_service import SparkIcebergService, SparkIcebergServiceException


class IcebergConnectorException(Exception):
    pass


class DataProfilingConnector(DataProfilingInterface):
    @staticmethod
    def profile_data(spark, dataframe: DataFrameReader, parameters: dict):
        """
        Pull the data from the flat file stored.
        Args:
            parameters (dict): The parameters to create the data profiling.
        Raises:
            DataProfilingConnector: An exception if an error occurred.
        """
        try:
            SparkIcebergService.table_exists(spark, dataframe, parameters)

            return SparkService.create_profile_data_from_table(dataframe, parameters)
        except SparkServiceException as spark_error:
            raise SparkIcebergServiceException(spark_error) from spark_error
