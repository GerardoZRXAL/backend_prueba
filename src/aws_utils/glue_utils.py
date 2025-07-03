# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        utility / glue_utils
Descripcion: 	Utileria para utilizar el servicio de Glue Jobs.
Modificacion:
                2024-01-15 - Creación
===================================================================================================
"""
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ValidationError, ParamValidationError
from loggers.aws_log_service import AWSLogService


class GlueUtilsException(Exception):
    pass


class GlueUtils:
    @staticmethod
    def update_table(database_name, table_input):
        """
        Function that updates a metadata table in the Data Catalog.
        Args:
            database_name(str): The name of the catalog database in which the table resides.
            table_input(json): The json that contains the updated metadata for the table in the catalog.
        Return:
            response(json): A json object with the values of the results execution.
        """
        config = Config(retries={"max_attempts": 5, "mode": "standard"})
        client = boto3.client("glue", region_name="us-east-1", config=config)
        response = client.update_table(
            DatabaseName=database_name, TableInput=table_input
        )
        return response

    @staticmethod
    def get_table(database_name, table_name):
        """
        Function to retrieves the Table definition in a Data Catalog for a specified table.
        Args:
            database_name(str): The name of the catalog database in which the table resides.
            table_input(json): The name of the table for which to retrieve the definition.
        Return:
            response(json): A json object with the values of the results execution.
        """
        config = Config(retries={"max_attempts": 3, "mode": "standard"})
        client = boto3.client("glue", region_name="us-east-1", config=config)
        response = client.get_table(DatabaseName=database_name, Name=table_name)
        return response

    @staticmethod
    def delete_old_table_versions(database_name, table_name, region="us-east-1", versions_to_keep=10):
        """
        Function to delete old versions of a table in the Glue Data Catalog, keeping the most recent versions.

        Args:
            database_name (str): The name of the catalog database where the table resides.
            table_name (str): The name of the table to manage versions.
            versions_to_keep (int): Number of most recent versions to keep. Defaults to 10.

        Return:
            dict: Summary of the deletion operation including:
                - total_versions: Total number of versions found
                - versions_deleted: Number of versions deleted
                - versions_kept: Number of versions kept
        """
        try:
            logging = AWSLogService()
            logging.log_info(
                {
                    "module": "GlueUtils.delete_old_table_versions",
                    "log_output_msg": (
                        f"*****Deled table verion from the table {database_name}.{table_name}."
                    ),
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            config = Config(retries={"max_attempts": 3, "mode": "standard"})
            client = boto3.client("glue", region_name=region, config=config)

            all_versions = []
            next_token = None

            while True:
                if next_token:
                    response = client.get_table_versions(
                        DatabaseName=database_name,
                        TableName=table_name,
                        NextToken=next_token
                    )
                else:
                    response = client.get_table_versions(
                        DatabaseName=database_name,
                        TableName=table_name
                    )

                all_versions.extend(response['TableVersions'])

                next_token = response.get('NextToken')
                if not next_token:
                    break

            all_versions.sort(key=lambda x: int(x['VersionId']), reverse=True)

            versions_to_delete = all_versions[versions_to_keep:]
            version_ids_to_delete = [v['VersionId'] for v in versions_to_delete]

            batch_size = 100
            for i in range(0, len(version_ids_to_delete), batch_size):
                batch = version_ids_to_delete[i:i + batch_size]
                if batch:
                    try:
                        restul = client.batch_delete_table_version(
                            DatabaseName=database_name,
                            TableName=table_name,
                            VersionIds=batch
                        )
                        logging.log_info(
                            {
                                "module": "GlueUtils.delete_old_table_versions",
                                "log_output_msg": "Status delete bath",
                                "dateil": f"Status code: {restul.get('ResponseMetadata').get('HTTPStatusCode')}",
                                "@LEVEL": "DEBUG",
                            }
                        )
                    except ClientError as error:
                        logging.log_info(
                            {
                                "module": "GlueUtils.delete_old_table_versions",
                                "log_output_msg": "Error batch delete",
                                "dateil": f"{error}",
                                "@LEVEL": "DEBUG",
                            }
                        )

            logging.log_info(
                {
                    "module": "GlueUtils.delete_old_table_versions",
                    "log_output_msg": (
                        f"*****Deled table verion from the table {database_name}.{table_name}."
                    ),
                    "@DETAIL": {
                        "total_versions": len(all_versions),
                        "versions_deleted": len(version_ids_to_delete),
                        "versions_kept": len(all_versions) - len(version_ids_to_delete)
                    },
                    "status": "Finished",
                    "@LEVEL": "DEBUG",
                }
            )
            return 'SUCCESS'
        except ValidationError as e:
            return {
                "STATUS": 'FAILED',
                "error": e
            }
        except ParamValidationError as error_message:
            return {
                "STATUS": 'FAILED',
                "error": error_message
            }
