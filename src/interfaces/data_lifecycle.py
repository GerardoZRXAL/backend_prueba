# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Adolfo Benitez Herrera
Proyecto:       Datalake Back
Proceso:        API
Descripcion: 	Clase encargada de definir los metodos principales de la clase para aplicar el proceso de data_lifecycle.
Modificacion:
                2025-03-26 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod


class DataLifecycleInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def drop_and_create_archive_table(spark, query_drop_table, create_table_query):
        """
        Deletes the work table and creates a new one with the data to archive.
        """

    @staticmethod
    @abstractmethod
    def archive_data(spark, sql_file):
        """
        Process used to delete the old data being archived.
        """
