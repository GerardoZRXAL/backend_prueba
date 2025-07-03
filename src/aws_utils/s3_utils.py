# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        aws_utils / s3_utils
Descripcion: 	Utileria para conectarse a los servicios de AWS.
Modificacion:
                2024-02-09 - Creación
===================================================================================================
"""
import boto3
from botocore.exceptions import ClientError
from loggers.aws_log_service import AWSLogService


class s3UtilsException(Exception):
    pass


class s3Utils:
    @staticmethod
    def download_file_s3(file_path, bucket, object_key):
        """
        Function to upload an object to a s3 route.
        Args:
            file_path(str) : The path when the file is saved.
            bucket(str): the bucket where the object is.
            object_key(str): the object to delete.
        return:
            None
        """
        client = boto3.client("s3")
        try:
            logging = AWSLogService()
            client.download_file(bucket, object_key, file_path)
            logging.log_info(
                {
                    "module": "s3Utils.download_file_s3",
                    "log_output_msg": (
                        f"***** Download object '{object_key}' from bucket '{bucket}' to {file_path}."
                    ),
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
        except ClientError as error:
            logging.log_error(
                {
                    "module": "s3Utils.download_file_s3",
                    "log_output_msg": (
                        f"***** Error to Download object '{object_key}' from bucket '{bucket}'."
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise s3UtilsException(error) from error
