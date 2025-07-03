# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        secret_manager
Descripcion: 	Clase encargada de definir los metodos principales de la clase secrets.
Modificacion:
                2024-01-25 - Creación
===================================================================================================
"""
from abc import ABC, ABCMeta, abstractmethod


class SecretInterface(ABC):
    __metaclass__ = ABCMeta

    @abstractmethod
    def _get_client(self, region_name: str) -> object:
        """
        Get the client from the secret service.
        """

    @abstractmethod
    def get_secret(self, secret: str, region_name: str) -> str:
        """
        Get the secret value.
        """
