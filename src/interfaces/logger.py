# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        logger
Descripcion: 	Clase encargada de definir los metodos principales de la clase logger.
Modificacion:
                2024-01-25 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod


class LoggerInterface(ABC):
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def log_info(message_dicts: dict):
        """
        This function will log the info logs.
        """

    @staticmethod
    @abstractmethod
    def log_error(errors):
        """
        This function will log the error logs.
        """
