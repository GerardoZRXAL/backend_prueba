# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        document_db
Descripcion: 	Clase encargada de definir los metodos principales de la clase DynamoDB.
Modificacion:
                2024-01-25 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod


class DocumentDbInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def get_data_from_table(table_name: str, key: str, value: str, region_name: str):
        """
        This function will fetch the records from the dynamo table.
        """

    @staticmethod
    @abstractmethod
    def set_update_values_in_dynamodb(
        table_name: str,
        key_value: str,
        process_name: str,
        init_delta_value: str,
        end_delta_value: str,
    ):
        """
        This function will update items in the DynamoDB table.
        """
