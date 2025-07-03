# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        connector
Descripcion: 	Clase encargada de definir los metodos principales de la clase conector.
Modificacion:
                2024-01-25 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod
from pyspark.sql.readwriter import DataFrameReader


class ConnectorInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def pull_data(spark, parameters: dict):
        """
        Pull the from the datasource.
        """

    @staticmethod
    @abstractmethod
    def save_data(
        dataframe: DataFrameReader, location: str, data_format: str, mode: str, partition_cols: list
    ):
        """
        Persist the data in a desired format and location.
        """
