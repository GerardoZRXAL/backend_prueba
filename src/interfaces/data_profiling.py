# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Adolfo Benitez Herrera
Proyecto:       Datalake Back
Proceso:        Data Profiling
Descripcion: 	Clase encargada de definir los metodos principales de la clase para manejar data profiling.
Modificacion:
                2025-03-18 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod
from pyspark.sql.readwriter import DataFrameReader


class DataProfilingInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def profile_data(spark, dataframe: DataFrameReader, parameters: dict):
        """
        Outputs the profiling based on the table proccesed.
        """
