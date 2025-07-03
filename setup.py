# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Fernando Colazo
Proyecto:       Datalake Back
Proceso:        Configuración del paquete.
Descripcion: 	Este script configura el paquete utilizando setuptools. Define el nombre, la versión
los paquetes incluidos y otras configuraciones necesarias.
Modificacion:
                2024-08-11 - Creación
===================================================================================================
"""

from setuptools import setup, find_packages

setup(
    name="datalake_back",
    version="0.1.0",
    packages=find_packages(
        where="src",
        include=[
            "aws_utils",
            "connectors",
            "constants",
            "loggers",
            "document_db_manager",
            "interfaces",
            "secrets_managers",
            "services",
            "utils",
        ],
    ),
    package_dir={"": "src"},
    include_package_data=True,
)
