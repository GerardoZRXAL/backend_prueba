# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Xaldigital
Proyecto:       Datalake Back
Proceso:        common_utils
Descripcion: 	Utileria comunes utilizadas en el proyecto.
===================================================================================================
"""
import json
from json import JSONDecodeError
import os
import subprocess
from datetime import datetime, timedelta
import time
import pytz
from loggers.aws_log_service import AWSLogService


class CommonUtilsException(Exception):
    pass


class CommonUtils:
    @staticmethod
    def load_json_file(file_path):
        """
        This function loads a file into a json object
        Args:
            file_path (str): The path where the file is stored
        Raises:
            CommonUtilsException: If the file cannot be loaded or does not exist
        Returns:
            [dict]: Json object with the content of the file
        """
        log_utils = AWSLogService()
        try:
            with open(file_path, encoding="utf-8") as json_file:
                return json.load(json_file)
        except (FileNotFoundError, JSONDecodeError) as error:
            log_utils.log_error(
                {
                    "module": "CommonUtils.load_json_file",
                    "log_output_msg": (
                        f"The was an error {error} reading the json file"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise CommonUtilsException(error) from error

    @staticmethod
    def get_environment_variable(variable_name: str):
        """
        Function to get the environment variable
        Args:
            variable_name (str): The variable name
        Raises:
            CommonUtilsException: If the variable does not exist this exception will be raised
        Returns:
            [str]: The value from the environment variable
        """
        log_utils = AWSLogService()
        if os.environ.get(variable_name):
            return os.environ.get(variable_name)
        log_utils.log_error(
            {
                "module": "CommonUtils.get_environment_variable",
                "log_output_msg": (f"The env variable {variable_name} does not exist"),
                "status": "Error",
                "@LEVEL": "ERROR",
            }
        )
        raise CommonUtilsException(f"The env variable {variable_name} does not exist")

    @staticmethod
    def get_file_name(file_path: str) -> str:
        """
        Get the file name from a path.
        Args:
            file_path (str): The file's path.
        Returns:
            [str]: The file's name.
        """
        return os.path.basename(file_path)

    @staticmethod
    def get_elapsed_seconds(start_time, decimals=3):
        """
        Function to get the elapsed time
        Args:
            start_time (float): millis fron time.time()
            decimals (int): optional, default=3, desired precision
        Returns:
            The elapsed time in seconds, i.e. 2.555
        """
        return round(time.time() - start_time, decimals)

    @staticmethod
    def read_file(file_path):
        """
        Function to read the a filat file.
        Args:
            file_path (str): The path where the file its saved
        Returns:
            file_content(str): The content of the file.
        """
        try:
            with open(file_path, "r") as file:
                file_content = file.read()
            return file_content
        except FileNotFoundError as error:
            raise CommonUtilsException(f"The file does not exist {error}") from error

    @staticmethod
    def kill_yarn_application():
        """
        Function to kill a YARN application.
        Args:
            None.
        Returns:
            None
        """
        log_utils = AWSLogService()
        kill_application_command = (
            "for x in $(yarn application -list -appStates RUNNING | awk 'NR > 2 { print $1 }');"
            " do yarn application -kill $x; done"
        )
        log_utils.log_info(
            {
                "module": "CommonUtils.kill_yarn_application",
                "log_output_msg": f"Killing YARN Application: {kill_application_command}",
                "status": "Running",
                "@LEVEL": "DEBUG",
            }
        )
        try:
            os.system(kill_application_command)
        except OSError as error:
            log_utils.log_error(
                {
                    "module": "CommonUtils.kill_yarn_application",
                    "log_output_msg": f"There aren't any application running Error [{error}]",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )

    @staticmethod
    def delete_path_haddop(path_hdfs):
        """
        Function to delete the data from hdfs.
        Args:
            path_hdfs (str): A string with the name of the table.
        Returns:
            None
        """
        log_utils = AWSLogService()
        result = subprocess.call(["hadoop", "fs", "-rm", "-r", "-f", path_hdfs])
        if result != 0:
            raise CommonUtilsException(f"Error to delete the path HDFS [{path_hdfs}]")
        log_utils.log_info(
            {
                "module": "CommonUtils.delete_path_haddop",
                "log_output_msg": f"The path was deleted correctly [{path_hdfs}]",
                "status": "Running",
                "@LEVEL": "DEBUG",
            }
        )

    @staticmethod
    def run_cmd(command_run, time_out=None):
        """
        Function to execute a bash command in python.
        Args:
            command_run (str): The command to execute.
        Returns:
            [boolean] A flag that show if the command run properly.
        """
        log_utils = AWSLogService()
        log_utils.log_info(
            {
                "module": "CommonUtils.run_cmd",
                "log_output_msg": f"Running system command: {command_run}",
                "status": "Running",
                "@LEVEL": "DEBUG",
            }
        )
        try:
            subprocess.run(command_run, timeout=time_out, check=False)
            return True
        except subprocess.TimeoutExpired as e:
            log_utils.log_error(
                {
                    "module": "CommonUtils.run_cmd",
                    "log_output_msg": f"Timeout Expired error {e}",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            CommonUtils.kill_yarn_application()
            return False

    @staticmethod
    def copy_data_hdfs_to_s3(path_hdfs, path_s3):
        """
        Function to copy the data from hdfs to s3.
        Args:
            path_hdfs (str): A string with the name of the table.
            path_s3 (str): The type of execution.
        Returns:
            None
        """
        log_utils = AWSLogService()
        result = subprocess.call(["hadoop", "distcp", path_hdfs, path_s3])
        if result != 0:
            raise NameError(f"Error to copy the path HDFS [{path_hdfs}] to [{path_s3}]")
        log_utils.log_info(
            {
                "module": "CommonUtils.copy_data_hdfs_to_s3",
                "log_output_msg": f"The path was copied correctly [{path_hdfs}] to [{path_s3}]",
                "status": "Running",
                "@LEVEL": "DEBUG",
            }
        )

    @staticmethod
    def date_minus_days(source_date: str, num_days: int, date_format: str, timezone="America/Mexico_City") -> str:
        """
        This method allows adding or subtracting days from a date.
        Args:
            source_date (date): The initial date
            num_days (int): The number of days.
            date_format (str): The format of the date that the string will have
            timezone (str, optional): Timezone for the date and time operations, default is "UTC-6"
        Returns:
            [str]: The date with the specified number of days added or subtracted.
        """
        tz = pytz.timezone(timezone)
        aux_date = datetime.strptime(source_date, date_format)
        aux_date = tz.localize(aux_date) if aux_date.tzinfo is None else aux_date
        target_date = aux_date + timedelta(days=num_days)
        return target_date.strftime(date_format)

    @staticmethod
    def get_spark_sql_type(data_type):
        """
        This method allows get the mapping for data type between apache iceberg and spark.
        Args:
            data_type (str): The data type to map.
        Returns:
            [str]: The equivalent data type for apache iceberg
        """
        type_mapping = {
            "stringtype()": "STRING",
            "integertype()": "INT",
            "longtype()": "LONG",
            "doubletype()": "DOUBLE",
            "floattype()": "FLOAT",
            "booleantype()": "BOOLEAN",
            "datetype()": "DATE",
            "timestamptype()": "TIMESTAMP"
        }
        return type_mapping.get(str(data_type).lower(), "STRING")

    @staticmethod
    def calculate_date_from_days(days):
        """
        Function to calculates a date by adding or subtracting days from the current date.
        Args:
            data_type (str): Number of days to add (positive) or subtract (negative)
                from current date
        Returns:
            [json]: The json object with the date in YYYY MM DD format.
        """
        current_date = datetime.now()
        result_date = current_date + timedelta(days=days)
        return {
            "year": result_date.strftime('%Y'),
            "month": result_date.strftime('%m'),
            "day": result_date.strftime('%d')
        }

    @staticmethod
    def parse_date_components(date_string):
        """
        Function to extracts year, month, and day from a date string in 'YYYY-MM-DD' format.
        Args:
            date_string (str): Date in 'YYYY-MM-DD' format
        Returns:
            tuple: (year, month, day) as integers
        """
        date_obj = datetime.strptime(date_string, '%Y-%m-%d')
        return {
            "year": date_obj.strftime('%Y'),
            "month": date_obj.strftime('%m'),
            "day": date_obj.strftime('%d')
        }
