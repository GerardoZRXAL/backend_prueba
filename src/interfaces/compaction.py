# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        Compaction
Descripcion: 	Clase encargada de definir los metodos principales de la clase compactacion.
Modificacion:
                2024-01-25 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod
from pyspark.sql.readwriter import DataFrameReader


class CompactionInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def read_data(spark, file_path: str, data_format: str, parameters: dict):
        """
        Read the data from the datasource.
        """

    @staticmethod
    @abstractmethod
    def insert_data(spark, dataframe: DataFrameReader, parameters: dict):
        """
        Persist the data in a desired format and location.
        """
