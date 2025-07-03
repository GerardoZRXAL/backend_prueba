# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        aws_dynamo_db
Descripcion: 	Utileria para acceder a la informacion de Dynamo DB.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key
from interfaces.document_db import DocumentDbInterface
from loggers.aws_log_service import AWSLogService


class AWSDynamoDbException(Exception):
    pass


class AWSDynamoDb(DocumentDbInterface):
    @staticmethod
    def get_data_from_table(
        table_name: str, key: str, value: str, region_name: str
    ) -> dict:
        """
        This method fetch the records from the dynamo table
        Args:
            table_name (str): The table's name from dynamo
            key (str): The key from the table
            value (str): The value from the key

        Returns:
            [dict]: A list with the results fetched from the table
        """
        logging = AWSLogService()
        try:
            dynamodb = boto3.resource("dynamodb", region_name=region_name)
            table_name = dynamodb.Table(table_name)
            response = table_name.query(KeyConditionExpression=Key(key).eq(value))
            logging.log_info(
                {
                    "module": "AWSDynamoDb.get_data_from_table",
                    "log_output_msg": f"**** Reading the dynamo table {table_name} ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            return response["Items"][0]
        except botocore.exceptions.ClientError as error:
            logging.log_error(
                {
                    "module": "AWSDynamoDb.get_data_from_table",
                    "log_output_msg": (
                        f"***** Error rerading the dynamo table {table_name}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise AWSDynamoDbException(
                f"An error ocurred trying to get item from Dynamo table: {error}"
            ) from error

    @staticmethod
    def get_all_data_from_table(table_name: str) -> dict:
        """
        This method fetch the records from the dynamo table
        Args:
            table_name (str): The table's name from dynamo
            key (str): The key from the table
            value (str): The value from the key

        Returns:
            [dict]: A list with the results fetched from the table
        """
        logging = AWSLogService()
        try:
            dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
            table = dynamodb.Table(table_name)
            response = table.scan()
            logging.log_info(
                {
                    "module": "AWSDynamoDb.get_all_data_from_table",
                    "log_output_msg": f"**** Reading all elements of the table {table_name} ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            return response["Items"]
        except botocore.exceptions.ClientError as error:
            logging.log_error(
                {
                    "module": "AWSDynamoDb.get_all_data_from_table",
                    "log_output_msg": (
                        f"***** Error renamed all elements of the table {table_name}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise AWSDynamoDbException(
                f"An error ocurred trying to get all items from Dynamo table: {error}"
            ) from error

    @staticmethod
    def update_item_from_table(table_name, key_name, key_value, key_name_update, value_update):
        """
        Allows you to update dynamo items.
        Args:
            table_name (str): The table's name from dynamo
            key_name (str): The name of the key defined for the DynamoDB table must be provided.
            key_value (str): The value of the ID for the DynamoDB table that you want to update must be specified.
            key_name (str): The name of the key that will be updated.
            key_name (str): The new value that will be assigned to the key to update.
        Raises:
        Returns:
            An integer value with the status code of the execution on the DynamoDB table.
        """
        logging = AWSLogService()
        try:
            dynamo = boto3.resource("dynamodb", region_name="us-east-1")
            table = dynamo.Table(table_name)
            response = table.update_item(
                Key={
                    key_name: key_value
                },
                UpdateExpression=f"set {key_name_update}=:val_dat",
                ExpressionAttributeValues={
                    ':val_dat': value_update
                }
            )
            logging.log_info(
                {
                    "module": "AWSDynamoDb.update_item_from_table",
                    "log_output_msg": (
                        f"**** Update the item {key_name_update} for the "
                        f"dynamo table {table_name} with the follow value {value_update} ****"
                    ),
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            return response.get('ResponseMetadata').get('HTTPStatusCode')
        except botocore.exceptions.ClientError as error:
            logging.log_error(
                {
                    "module": "AWSDynamoDb.update_item_from_table",
                    "log_output_msg": (
                        f"***** Error uploading the table {table_name}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise AWSDynamoDbException(
                f"An error ocurred trying to get all items from Dynamo table: {error}"
            ) from error
