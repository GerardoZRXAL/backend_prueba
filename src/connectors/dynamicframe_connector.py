# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Felipe Vicera
Proyecto:       Datalake Back
Proceso:        Connectors / DynamicFrame
Descripcion: 	Utileria para realizar operaciones con dynamicframe de glue.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
from interfaces.dynamicframe import DynamicFrameInterface
from services.dynamicframe_service import DynamicFrameService, DynamicFrameServiceException

class DynamicFrameConnectorException(Exception):
    pass

class DynamicFrameConnector(DynamicFrameInterface):
    @staticmethod
    def read_data(glueContext, parameters: dict, additional_read_options=None, type=None):
        """
        Pull the data from aws glue Datacatalog or another differente datasources.
        Args:
            glueContext (glue): glueContext object to manage the operations
            parameters (dict): The parameters to connect to the datasource.
            additional_read_options (dict): Additional options to read the data.
            type (str): The type of the datasource.
        Raises:
            DynamicFrameServiceException: An exception if an error occurred.
        Returns:
            DataFrameReader: The data in format of dataframe.
        """
        try:
            if type == "catalog":
                dynamic_frame = DynamicFrameService.read_data_frame_from_catalog(glueContext, parameters, additional_read_options)
            else:
                dynamic_frame = DynamicFrameService.read_data_frame_from_options(glueContext, parameters)
            return dynamic_frame
        except DynamicFrameServiceException as dynamicframe_error:
            raise DynamicFrameConnectorException(dynamicframe_error) from dynamicframe_error

    @staticmethod
    def insert_data(glueContext, dynamic_frame, parameters: dict):
        """
        Saves the data in the location and format defined.
        Args:
            glueContext (glue): glueContext object to manage the operations
            dynamic_frame (DataFrame): The data in format of dataframe.
            parameters (dict): The parameters to connect to the datasource.
        Raises:
            DynamicFrameServiceException: An exception if an error occurred.
        Returns:
            None
        """
        try:
            DynamicFrameService.write_data_frame_from_options(glueContext, dynamic_frame, parameters)
        except DynamicFrameServiceException as dynamicframe_error:
            raise DynamicFrameConnectorException(dynamicframe_error) from dynamicframe_error
