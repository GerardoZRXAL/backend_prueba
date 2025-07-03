# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Adolfo Benitez Herrera
Proyecto:       Datalake Back
Proceso:        API
Descripcion: 	Clase encargada de definir los metodos principales de la clase para manejar API ingestion.
Modificacion:
                2024-12-11 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod
from pyspark.sql.readwriter import DataFrameReader


class APIInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def process_api_data(spark, api_config: dict, pagination_type):
        """
        Process API data using the provided configuration.
        """

    @staticmethod
    @abstractmethod
    def insert_data(dataframe: DataFrameReader, parameters: dict):
        """
        Saves the data in the s3 location defined and with parquet format.
        """
