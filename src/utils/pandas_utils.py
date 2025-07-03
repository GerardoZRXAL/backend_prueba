# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        Utility / pandas_utils
Descripcion: 	Utileria para conectarse a los servicios de AWS.
Modificacion:
                2024-01-15 - Creación
===================================================================================================
"""
import pandas as pd
from loggers.aws_log_service import AWSLogService


class PandasUtilsException(Exception):
    pass


class PandasUtils:
    @staticmethod
    def read_excel(pandas_args):
        """
        Function to read an Excel File using pandas.
        Args:
            file_path(str): The json with the arguments to read.
        Returns:
            df(pd.DataFrame): The content of the file loaded into a dataframe.
        """
        log_utils = AWSLogService()
        try:
            df = pd.read_excel(**pandas_args)
            return df
        except (
            FileNotFoundError,
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
        ) as error:
            log_utils.log_error(
                {
                    "module": "PandasUtils.read_excel",
                    "log_output_msg": (f"The was an error {error} reading the file"),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise PandasUtilsException(error) from error
