# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        aws_log_service
Descripcion: 	Utileria para crear los logs.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
import logging
import datetime
import watchtower
from interfaces.logger import LoggerInterface


class AWSLogService(LoggerInterface):
    class __AWSLogService:
        def __init__(self, **kwargs):
            logging.basicConfig(
                level="INFO",
                handlers=[logging.StreamHandler()],
                format="%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            mexico_tz = datetime.timezone(datetime.timedelta(hours=-6))
            execute_time = datetime.datetime.now(mexico_tz)
            format_dt = datetime.datetime.strftime(
                execute_time, format="%Y-%m-%d %H_%M_%S"
            )
            self.logger = logging.getLogger(kwargs.get("process"))
            self.logger.addHandler(
                watchtower.CloudWatchLogHandler(
                    log_group_name=kwargs.get("group_name"),
                    log_stream_name=f"{kwargs.get('process')}/{kwargs.get('process_id')}/{format_dt}",
                )
            )

    instance = None

    def __init__(self, **kwargs):
        if not AWSLogService.instance:
            AWSLogService.instance = AWSLogService.__AWSLogService(
                process=kwargs.get("process", "process"),
                group_name=kwargs.get("log_group_name", "datalake-back"),
                process_id=kwargs.get("process_id", "default_id"),
            )

    @staticmethod
    def log_info(message_dicts: dict):
        """
        Function to log the messages with descriptive information
        Args:
            message_dicts (dic): Key/value messages
        """
        message_dicts["type"] = "INFO"
        AWSLogService.instance.logger.setLevel(logging.INFO)
        AWSLogService.instance.logger.info(message_dicts)

    @staticmethod
    def log_error(errors):
        """
        Function to log the messages with error
        Args:
            errors (str): Message with the error
        """
        AWSLogService()
        AWSLogService.instance.logger.setLevel(logging.ERROR)
        AWSLogService.instance.logger.error("ERROR_DLB: %s", errors)
