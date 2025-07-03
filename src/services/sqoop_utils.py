# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       POC HISTORY
Proceso:        Utils / sqoop_utils
Descripcion: 	Utileria para crear los comando de sqoop para ejecutar.
Modificacion:
                2022-05-17 - Creación
===================================================================================================
"""
from loggers.aws_log_service import AWSLogService
import constants.aws_constants as Constants


class SqoopUtils:
    @staticmethod
    def get_sqoops_command(table_info, secret_dict):
        """
        Function to create sqoop commands.
        Args:
            table_name (str): A string with the name of the table
            extraction_mode (str): The type of execution.
            num_mappers (str): Use n map tasks to import in parallel.
            split_by (str): Column of the table used to split work units.
            start_date (str): Date to use in incremental execution.
            end_date (str): Date to use in incremental execution.
        Returns:
            [list]: The sqoop command to execute.
        """
        log_utils = AWSLogService()
        host = secret_dict.get("host")
        port = secret_dict.get("port")
        username = secret_dict.get("username")
        p4ssw0rd = secret_dict.get("password")
        dbname = secret_dict.get("dbname")
        td_hdfs = (
            f"{Constants.HDFS_RAW_FOLDER}{table_info.get('schema')}_{table_info.get('table_name')}"
        )
        if secret_dict.get("type") == "oracle":
            sid = secret_dict.get("sid")
            connection = f"jdbc:oracle:thin:{username}/{p4ssw0rd}@//{host}:{port}/{sid}"
        else:
            connection = f"jdbc:{secret_dict.get('type')}://{host}:{port}/{dbname}"
        select_custom_fields = table_info.get("select_custom_fields")
        if select_custom_fields is not None:
            select_value = f'{select_custom_fields}'
        else:
            select_value = 't.*'

        encoding = table_info.get("encoding")
        if encoding is not None:
            connection = connection + f'?characterSetResults={encoding}'

        if table_info.get("temp_table") is not None:
            table_name = table_info.get("temp_table")
        else:
            table_name = table_info.get("table_name")

        if table_info.get("extraction_mode") == "FULL_LOAD":
            sqoop_command = [
                "sqoop",
                "import",
                "--connect",
                f"{connection}",
                "--username",
                f"{username}",
                "--password",
                f"{p4ssw0rd}",
                "--delete-target-dir",
                "-m",
                "1",
                "--query",
                (
                    f"SELECT {select_value} FROM "
                    f"{table_info.get('schema')}.{table_name} t"
                    " WHERE $CONDITIONS"
                ),
                "--target-dir",
                f"{td_hdfs}",
                "--as-parquetfile",
                "--null-string",
                "",
                "--null-non-string",
                "",
                "--verbose",
            ]
        elif table_info.get("extraction_mode") == "INCREMENTAL":
            sqoop_command = [
                "sqoop",
                "import",
                "-Dorg.apache.sqoop.splitter.allow_text_splitter=true",
                "--connect",
                f"{connection}",
                "--username",
                f"{username}",
                "--password",
                f"{p4ssw0rd}",
                "--delete-target-dir",
                "-m",
                f"{table_info.get('num_mappers')}",
                "--query",
                (
                    f"SELECT {select_value} FROM "
                    f"{table_info.get('schema')}.{table_name} t WHERE "
                    f"t.{table_info.get('columns_incremental')} BETWEEN '{table_info.get('start_date')}' "
                    f"AND '{table_info.get('end_date')}' "
                    "AND $CONDITIONS"
                ),
                "--target-dir",
                f"{td_hdfs}",
                "--as-parquetfile",
                "--null-string",
                "",
                "--null-non-string",
                "",
                "--verbose",
            ]
            if table_info.get('split_by'):
                sqoop_command.append("--split-by")
                sqoop_command.append(table_info.get('split_by'))
        log_utils.log_info(
            {
                "module": "SqoopUtils.get_sqoops_command",
                "log_output_msg": f"Getting sqoop command {sqoop_command}",
                "status": "Running",
                "@LEVEL": "DEBUG",
            }
        )
        return sqoop_command
