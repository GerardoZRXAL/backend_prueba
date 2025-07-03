# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        aws_utils / sns_utils
Descripcion: 	Utileria para conectarse a los servicios de AWS.
Modificacion:
                2024-02-09 - Creación
===================================================================================================
"""
import json
import boto3
from botocore.exceptions import ClientError
from loggers.aws_log_service import AWSLogService


class SnsUtilsException(Exception):
    pass


class SnsUtils:
    @staticmethod
    def publish_to_sns(sns_topic_arn, sub, msg):
        """
        Function to send email notifications using AWS SNS.
        Args:
            sns_topic_arn(str): The arn of the topic in AWS SNS.
            sub(str): The subject of the email.
            msg(str): The message of the email.
        return:
            None
        """
        try:
            logging = AWSLogService()
            message = {"foo": "bar"}
            client = boto3.client("sns", region_name="us-east-1")
            client.publish(
                TargetArn=sns_topic_arn,
                Message=json.dumps({"default": json.dumps(message), "email": msg}),
                Subject=sub,
                MessageStructure="json",
            )
            logging.log_info(
                {
                    "module": "SnsUtils.publish_to_sns",
                    "log_output_msg": (
                        f"***** The message was published to the topic '{sns_topic_arn}'. *****"
                    ),
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
        except ClientError as error:
            logging.log_error(
                {
                    "module": "SnsUtils.publish_to_sns",
                    "log_output_msg": (
                        f"***** Error to send the message to the topic '{sns_topic_arn}'. *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SnsUtilsException(error) from error
