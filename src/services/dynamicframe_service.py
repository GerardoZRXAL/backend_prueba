# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Felipe Vicera
Proyecto:       Datalake Back
Proceso:        services / dynamicframe_service
Descripcion: 	Utileria para realizar las operaciones con DynamicFrame de Glue.
Modificacion:
                2023-10-05 - Creación
===================================================================================================
"""
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException, StreamingQueryException
from loggers.aws_log_service import AWSLogService

class DynamicFrameServiceException(Exception):
    pass

class DynamicFrameService:
    @staticmethod
    def read_data_frame_from_options(glueContext, parameters):
        """
        This function reads a dataframe using from_options method
        Args:
            glueContext (GlueContext): The Glue context that will be used to read the dataframe
            parameters (dict): The parameters that will be used to read the dataframe
        Returns:
            data_frame (dataframe): The dataframe that will be inserted
        """
        logging = AWSLogService()
        try:
            data_frame = glueContext.create_data_frame.from_options(
                connection_type=parameters.get('connection_type'),
                connection_options=parameters.get('connection_options'),
                transformation_ctx=parameters.get('transformation_ctx')
            )
            return data_frame
        except (AnalysisException, StreamingQueryException, AttributeError) as error:
            logging.log_error(
                {
                    "module": "DynamicFrameservice.read_data_frame_from_options",
                    "log_output_msg": (
                        f"***** Error to read the df from {parameters.get('connection_type')}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise DynamicFrameServiceException(error) from error

    @staticmethod
    def read_data_frame_from_catalog(glueContext, parameters, additional_read_options):
        """
        This function reads a dataframe from a catalog
        Args:
            glueContext (GlueContext): The Glue context that will be used to read the dataframe
            glue_catalog (str) : AWS Glue DataCatalog
            additional_read_options (dict): The addigional options that will be used to read the dataframe
            logging (AWSLogService): The logging service that will be used to log the errors
        Returns:
            data_frame (dataframe): The dataframe that will be inserted
        """
        logging = AWSLogService()
        try:
            database = parameters.get("database")
            table_name = parameters.get("table_name")
            data_frame = glueContext.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table_name,
                predicate=parameters.get("predicate"),
                additional_options=additional_read_options,
                transformation_ctx=parameters.get("transformation_ctx"),
            )
            return data_frame
        except (AnalysisException, AttributeError) as error:
            logging.log_error(
                {
                    "module": "DynamicFrameservice.read_data_frame_from_catalog",
                    "log_output_msg": (
                        f"***** Error to read the df from datacatalog {database}.{table_name}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise DynamicFrameServiceException(error) from error

    @staticmethod
    def write_data_frame_from_options(glueContext, data_frame, parameters):
        """
        This function writes a dataframe using from_options method
        Args:
            data_frame (dataframe): The dataframe that will be inserted
            parameters (dict): The parameters that will be used to write the dataframe
        Returns:
            None
        """
        logging = AWSLogService()
        try:
            glueContext.write_dynamic_frame.from_options(
                frame=data_frame,
                connection_type=parameters.get("connection_type"),
                connection_options=parameters.get("connection_options"),
                transformation_ctx=parameters.get("transformation_ctx"),
            )
        except (AttributeError, Py4JJavaError, StreamingQueryException) as error:
            logging.log_error(
                {
                    "module": "DynamicFrameservice.write_data_frame_from_options",
                    "log_output_msg": (
                        f"***** Error to write the df to {parameters.get('connection_type')}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise DynamicFrameServiceException(error) from error
