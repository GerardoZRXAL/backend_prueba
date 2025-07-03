# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Felipe Vicera
Proyecto:       Datalake Back
Proceso:        DynamicFrame
Descripcion: 	Clase encargada de definir los metodos principales de la clase para manejar "dynamic frames".
Modificacion:
                2024-11-13 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod

class DynamicFrameInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def read_data(glueContext, parameters: dict, additional_read_options: dict, type: str):
        """
        Read the data from the datasource.
        """

    @staticmethod
    @abstractmethod
    def insert_data(glueContext, dynamic_frame, parameters: dict):
        """
        Persist the data in a desired format and location.
        """
