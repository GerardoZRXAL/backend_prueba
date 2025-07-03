# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        aws_secret_service
Descripcion: 	Utileria para conectarse al servicio de secret manager.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
import base64
import json
from json import JSONDecodeError
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from interfaces.secret_manager import SecretInterface
from loggers.aws_log_service import AWSLogService


class AWSSecretServiceException(Exception):
    pass


class AWSSecretService(SecretInterface):
    def __init__(self):
        self.client = None

    def _get_client(self, region_name: str) -> boto3.client:
        """
        Function to get the secret manager client from aws.
        Args:
            region_name (str): Desired region from aws.
        Returns:
            [boto3.client]: The secret manager client.
        """
        if self.client is None:
            session = boto3.session.Session()
            self.client = session.client(
                service_name="secretsmanager", region_name=region_name
            )
        return self.client

    def get_secret(self, secret: str, region_name: str) -> str:
        """
        Function to get the secret from aws.
        Args:
            secret (str): The secret name to retrieve from the secret manager.
            region_name (str): The region name where is the service.
        Raises:
            AWSSecretServiceException: If there is a problem trying to fetch the credentials from aws.
        Returns:
            [str]: The secret value in a string.
        """
        secret_client = self._get_client(region_name)
        logging = AWSLogService()
        try:
            response = secret_client.get_secret_value(SecretId=secret)
        except (ClientError, NoCredentialsError) as error:
            logging.log_error(
                {
                    "module": "AWSSecretService.get_secret",
                    "log_output_msg": (
                        f"***** Error getting the secret {secret}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise AWSSecretServiceException(error) from error
        if "SecretString" in response:
            response = response.get("SecretString")
            try:
                response = json.loads(response)
            except JSONDecodeError as error:
                logging.log_error(
                    {
                        "module": "AWSSecretService.get_secret",
                        "log_output_msg": (
                            f"***** Error with the json format: {error} *****"
                        ),
                        "status": "Error",
                        "@LEVEL": "ERROR",
                    }
                )
                raise AWSSecretServiceException(error) from error
        else:
            response = base64.b64decode(response.get("SecretBinary")).decode(
                "utf-8", "ignore"
            )

        return response
