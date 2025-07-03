# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        driver_compaction_iceberg
Descripcion: 	Clase encargada manejar las compactaciones con iceberg.
Modificacion:
                2024-01-25 - Creación
===================================================================================================
"""
import time
import sys
import pandas as pd
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from connectors.iceberg_connector import IcebergConnector
from document_db_manager.aws_dynamo_db import AWSDynamoDb
from loggers.aws_log_service import AWSLogService
from utils.common_utils import CommonUtils
from utils.pandas_utils import PandasUtils
from aws_utils.s3_utils import s3Utils
from aws_utils.glue_utils import GlueUtils
from services.spark_service import SparkService
from services.iceberg_service import SparkIcebergService
from services.data_mask_service import DataMasker


def cast_df_from_excel(df, dynamo_data, log_utils):
    """
    Function to cast a df using an excel file
    Args:
        dynamo_data(json): The data that contains the values for update the metadata.
    Returns:
        None
    """
    file_name = CommonUtils.get_file_name(dynamo_data.get("prefix_file_path"))
    s3Utils.download_file_s3(
        f"/tmp/{file_name}",
        dynamo_data.get("bucket_name"),
        dynamo_data.get("prefix_file_path"),
    )
    pandas_args = {
        "io": f"/tmp/{file_name}",
        "sheet_name": dynamo_data.get("excel_config").get("sheet_name"),
        "skiprows": int(dynamo_data.get("excel_config").get("skiprows")),
    }
    df_pandas = PandasUtils.read_excel(pandas_args)
    df_pandas = df_pandas.reset_index()
    source_columns = set(df.columns)

    maps_columns = {}
    # TIME_ZONE = "'America/Mexico_City'"
    for index, row in df_pandas.iterrows():
        print(index)
        print("data type", row.get("DATA TYPE"))
        if row.get("COLUMN NAME") not in source_columns:
            log_utils.log_error(
                {
                    "module": "driver_compaction_iceberg.cast_df_from_excel",
                    "key": f"***** The column {row.get('COLUMN NAME')} doesn't exists in the dataframe *****",
                    "@LEVEL": "DEBUG",
                }
            )
            continue
        cast_sentence = {}

        if pd.notnull(row.get("LONG")) and row.get("DATA TYPE") != "array":
            cast_sentence = {
                row.get(
                    "COLUMN NAME"
                ): f'cast(`{row.get("COLUMN NAME")}` as {row.get("DATA TYPE")}({row.get("LONG")}))'
            }
        elif row.get("DATA TYPE") == "timestamp":
            cast_sentence = cast_sentence = {
                row.get("COLUMN NAME"): f'to_timestamp(`{row.get("COLUMN NAME")}`)'
            }
        elif row.get("DATA TYPE") == "boolean":
            cast_sentence = cast_sentence = {
                row.get(
                    "COLUMN NAME"
                ): f'CASE WHEN {row.get("COLUMN NAME")} IS NULL OR {row.get("COLUMN NAME")} = "" THEN false ELSE CAST(`{row.get("COLUMN NAME")}` AS boolean) END'
            }
        elif row.get("DATA TYPE") == "array":
            element_type = row.get("LONG", "string")

            type_mapping = {
                "string": "array<string>",
                "integer": "array<integer>",
                "long": "array<long>",
                "bigint": "array<long>",
                "double": "array<double>",
                "boolean": "array<boolean>",
            }

            # Default to array<string> if type is not recognized
            cast_type = type_mapping.get(element_type, "array<string>")

            # Simplified casting approach
            cast_sentence = {
                row.get("COLUMN NAME"): f"CAST({row.get('COLUMN NAME')} AS {cast_type})"
            }
        elif row.get("DATA TYPE") == "double":
            cast_sentence = {
                row.get("COLUMN NAME"): f"CAST(`{row.get('COLUMN NAME')}` AS DOUBLE)"
            }
        else:
            cast_sentence = cast_sentence = {
                row.get(
                    "COLUMN NAME"
                ): f'cast(`{row.get("COLUMN NAME")}` as {row.get("DATA TYPE")})'
            }

        maps_columns.update(cast_sentence)
    return SparkService.add_dynamic_columns(df, maps_columns)


def update_metadata_table(dynamo_data):
    """
    Function to update the description of the table in glue.
    Args:
        dynamo_data(json): The data that contains the values for update the metadata.
    Returns:
        None
    """
    log_utils = AWSLogService()
    file_name = CommonUtils.get_file_name(dynamo_data.get("prefix_file_path"))
    s3Utils.download_file_s3(
        f"/tmp/{file_name}",
        dynamo_data.get("bucket_name"),
        dynamo_data.get("prefix_file_path"),
    )
    pandas_args = {
        "io": f"/tmp/{file_name}",
        "sheet_name": dynamo_data.get("excel_config").get("sheet_name"),
        "skiprows": int(dynamo_data.get("excel_config").get("skiprows")),
    }
    filter_column_id = dynamo_data.get("excel_config").get("table_column_name")
    filter_column_name = dynamo_data.get("excel_config").get("fields_column_name")
    description_columns = dynamo_data.get("excel_config").get("description_column_name")
    df = PandasUtils.read_excel(pandas_args)
    response = GlueUtils.get_table(
        dynamo_data.get("data_bases"), dynamo_data.get("table_name")
    )
    schema_info = []
    columns = response.get("Table").get("StorageDescriptor").get("Columns")
    df[filter_column_id] = df[filter_column_id].str.lower()
    df[filter_column_name] = df[filter_column_name].str.lower()
    for value in columns:
        try:
            df_filter = df.where(df[filter_column_name] == value.get("Name")).dropna(
                how="all"
            )
            value["Comment"] = df_filter[description_columns].values[0]
        except IndexError as error_msg:
            log_utils.log_error(
                {
                    "key": (f"The column {value.get('Name')} hasn't had a description"),
                    "error_msg": error_msg,
                    "process_name": "zz1_esh_l_asset_anlz",
                    "@LEVEL": "ERROR",
                }
            )
        schema_info.append(value)
    table_input = {
        "Name": response.get("Table").get("Name"),
        "Description": response.get("Table").get("Description", ""),
        "StorageDescriptor": {
            "Columns": schema_info,
            "Location": response.get("Table").get("StorageDescriptor").get("Location"),
            "InputFormat": response.get("Table")
            .get("StorageDescriptor")
            .get("InputFormat", ""),
            "OutputFormat": response.get("Table")
            .get("StorageDescriptor")
            .get("OutputFormat", ""),
            "Compressed": response.get("Table")
            .get("StorageDescriptor")
            .get("Compressed"),
            "NumberOfBuckets": response.get("Table")
            .get("StorageDescriptor")
            .get("NumberOfBuckets"),
            "SerdeInfo": response.get("Table")
            .get("StorageDescriptor")
            .get("SerdeInfo", {}),
            "SortColumns": response.get("Table")
            .get("StorageDescriptor")
            .get("SortColumns"),
            "StoredAsSubDirectories": response.get("Table")
            .get("StorageDescriptor")
            .get("StoredAsSubDirectories"),
        },
        "PartitionKeys": response.get("Table").get("PartitionKeys", []),
        "TableType": response.get("Table").get("TableType"),
        "Parameters": response.get("Table").get("Parameters"),
    }
    GlueUtils.update_table(dynamo_data.get("data_bases"), table_input)


def main():
    """
    Function to create the transformations.
    Args:
        None
    return:
        None
    """
    # @params: [JOB_NAME]
    execute_start_time = time.time()
    required_aruments = [
        "JOB_NAME",
        "STAGING_BUCKET",
        "RAW_BUCKET",
        "DYNAMO_DB_ID",
        "DYNAMO_DB_TABLE_NAME",
        "DYNAMO_DB_TABLE_NAME_METADATA",
        "LOG_GROUP_NAME",
    ]
    optional_arguments = ["FILES_TO_PROCESS"]
    args_req = getResolvedOptions(sys.argv, required_aruments)
    optional_values = {}
    for param in optional_arguments:
        try:
            optional_values[param] = getResolvedOptions(sys.argv, [param])[param]
        except GlueArgumentError:
            optional_values[param] = None
    args = {**args_req, **optional_values}
    # args = getResolvedOptions(
    #     sys.argv,
    #     [
    #         "JOB_NAME",
    #         "STAGING_BUCKET",
    #         "RAW_BUCKET",
    #         "DYNAMO_DB_ID",
    #         "DYNAMO_DB_TABLE_NAME",
    #         "DYNAMO_DB_TABLE_NAME_METADATA",
    #         "LOG_GROUP_NAME",
    #         "FILES_TO_PROCESS",
    #     ],
    # )
    log_utils = AWSLogService(
        process=args.get("JOB_NAME"),
        log_group_name=args.get("LOG_GROUP_NAME"),
        process_id=args.get("DYNAMO_DB_ID"),
    )
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    dynamo_data = AWSDynamoDb.get_data_from_table(
        args.get("DYNAMO_DB_TABLE_NAME"),
        "process_id",
        args.get("DYNAMO_DB_ID"),
        "us-east-1",
    )
    log_utils.log_info(
        {
            "@STAGE": "DATALAKE BACK",
            "@TASK": "Start Consulting",
            "@TABLE_NAME": dynamo_data.get("process_name"),
            "@EXECUTE_START_TIME": execute_start_time,
            "@EXECUTE_END_TIME": None,
            "@TOTAL_EXECUTION_TIME": None,
            "@STATUS": "RUNNING",
            "@DETAIL": "Execution of process to compact information",
            "@LEVEL": "INFO",
        }
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    datacatalog_name = "AwsDataCatalog"
    warehouse_path = (
        f'{args.get("STAGING_BUCKET")}/'
        f'{dynamo_data.get("staging_prefix")}/'
        f'tb_{dynamo_data.get("process_name")}/'
    )
    spark.conf.set(
        f"spark.sql.catalog.{datacatalog_name}",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    spark.conf.set(f"spark.sql.catalog.{datacatalog_name}.warehouse", warehouse_path)
    spark.conf.set(
        f"spark.sql.catalog.{datacatalog_name}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    spark.conf.set(
        f"spark.sql.catalog.{datacatalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    spark_context = spark.sparkContext
    spark_context.setLogLevel("INFO")

    if dynamo_data.get("spark_conf_casesensitive"):
        # Activar la sensibilidad a mayúsculas y minúsculas
        spark.conf.set("spark.sql.caseSensitive", "true")
    if dynamo_data.get("source_time_zone"):
        spark.conf.set(
            "spark.sql.session.timeZone", dynamo_data.get("source_time_zone")
        )

    file_path = []
    if args.get("FILES_TO_PROCESS"):
        file_path = [
            f"s3://{args.get('RAW_BUCKET')}/{dynamo_data.get('raw_prefix')}/{foie_process.strip()}"
            for foie_process in args.get("FILES_TO_PROCESS").split(",")
        ]
    else:
        file_path.append(
            f"s3://{args.get('RAW_BUCKET')}/{dynamo_data.get('raw_prefix')}/"
        )

    # Get spark read options spark
    read_options = dynamo_data.get(
        "spark_read_options_raw", {"recursiveFileLookup": "true"}
    )

    # df = IcebergConnector.read_data(spark, file_path, "parquet", read_options)
    # additional_options = {"recursiveFileLookup": "true"}

    if dynamo_data.get("partition_raw") is not None:
        if dynamo_data.get("partition_raw").get("type") == "R":
            date_start_json = CommonUtils.parse_date_components(
                dynamo_data.get("partition_raw").get("star_date")
            )
            date_end_json = CommonUtils.parse_date_components(
                dynamo_data.get("partition_raw").get("end_date")
            )
            predicate = (
                "(year>='"
                f"{date_start_json.get('year')}"
                "' and year<='"
                f"{date_end_json.get('year')}"
                "' and month>='"
                f"{date_start_json.get('month')}"
                "' and month<='"
                f"{date_end_json.get('month')}"
                "' and day>='"
                f"{date_start_json.get('day')}"
                "' and day<='"
                f"{date_end_json.get('day')}"
                "')"
            )
        else:
            date_json = CommonUtils.calculate_date_from_days(
                int(dynamo_data.get("partition_raw").get("days"))
            )
            predicate = (
                "(year=='"
                f"{date_json.get('year')}"
                "' and month=='"
                f"{date_json.get('month')}"
                "' and day>='"
                f"{date_json.get('day')}"
                "')"
            )
        dynamicFrame = glueContext.create_dynamic_frame.from_catalog(
            database=dynamo_data.get("partition_raw").get("database"),
            table_name=dynamo_data.get("partition_raw").get("table_name"),
            push_down_predicate=predicate,
            additional_options=read_options,
            transformation_ctx=f'{dynamo_data.get("process_name")}_raw_data',
        )
    elif dynamo_data.get("spark_read_legacy"):
        dynamicFrame = SparkService.read_from_file(
            spark,
            file_path,
            dynamo_data.get("raw_format_type", "parquet"),
            read_options,
        )
    else:
        dynamicFrame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": file_path, **read_options},
            format="parquet",
            transformation_ctx=f'{dynamo_data.get("process_name")}_raw_data',
        )

    # Verificar si es un DynamicFrame
    if isinstance(dynamicFrame, DynamicFrame):
        df = dynamicFrame.toDF()
    else:
        df = dynamicFrame
    count = df.count()
    df.printSchema()
    if count > 0:
        log_utils.log_info(
            {
                "@STAGE": "DATALAKE BACK",
                "@TABLE_NAME": dynamo_data.get("process_name"),
                "@TASK": "Enriching, cleaning and transforming data",
                "@STATUS": "RUNNING",
                "@DETAIL": f"Number of records to clean {count}",
                "@LEVEL": "INFO",
            }
        )
        if dynamo_data.get("raw_flatten_df"):
            df = SparkService.flatten_nested_columns(df, lower_columns=False)

        # To flatten zendesk api data
        raw_flatten_df_api = bool(dynamo_data.get("raw_flatten_df_api", False))
        separator = dynamo_data.get("raw_flatten_df_separator", "_")

        if raw_flatten_df_api:
            log_utils.log_info(
                {
                    "@STAGE": "DATALAKE BACK",
                    "@TABLE_NAME": dynamo_data.get("process_name"),
                    "@TASK": "Flatten data",
                    "@STATUS": "RUNNING",
                    "@DETAIL": f"Number of records to flatten {count}",
                    "@LEVEL": "INFO",
                }
            )
            df = SparkService.flatten_api_nested_columns(df, separator)

        if dynamo_data.get("raw_replace_spaces_with_underscores"):
            df = SparkService.replace_spaces_with_underscores(df)
        if dynamo_data.get("raw_normalize_col_types"):
            df = SparkService.normalize_column_types(df)
        if dynamo_data.get("raw_merge_duplicate_columns"):
            df = SparkService.merge_duplicate_columns(df)
        if dynamo_data.get("raw_lower_columns"):
            df = SparkService.lower_columns(df)

        df = SparkService.trim_string_columns(df)
        if dynamo_data.get("add_available_columns_list"):
            df = SparkService.add_available_columns_list(df)
        if dynamo_data.get("validate_existing_columns"):
            df = SparkService.validate_existing_columns(df, dynamo_data.get("validate_existing_columns"))
        if dynamo_data.get("cast_columns"):
            df = cast_df_from_excel(df, dynamo_data.get("cast_columns"), log_utils)
        if dynamo_data.get("add_columns"):
            df = SparkService.add_dynamic_columns(df, dynamo_data.get("add_columns"))
        if dynamo_data.get("renamed_columns"):
            df = SparkService.renamed_columns(df, dynamo_data.get("renamed_columns"))
        if dynamo_data.get("remove_columns"):
            df = SparkService.remove_columns(df, dynamo_data.get("remove_columns"))
        if dynamo_data.get("select_columns"):
            df = SparkService.select_columns(df, dynamo_data.get("select_columns"))
        if dynamo_data.get("remove_cols"):
            df = SparkService.remove_columns(df, dynamo_data.get("remove_cols"))

        if dynamo_data.get("raw_normalize_col_types"):
            df = SparkService.normalize_column_types(df)
        if dynamo_data.get("raw_merge_duplicate_columns"):
            df = SparkService.merge_duplicate_columns(df)
        if dynamo_data.get("raw_lower_columns"):
            df = SparkService.lower_columns(df)

        parameters_missing_cols = {
            "glue_catalog": datacatalog_name,
            "add_input_missing_columns": dynamo_data.get("add_input_missing_columns"),
            "data_bases": dynamo_data.get("data_bases"),
            "table_name": dynamo_data.get("process_name")
        }
        if dynamo_data.get('add_input_missing_columns'):
            df_complete = SparkIcebergService.add_missing_cols_input(spark, df, parameters_missing_cols)
            columns_without_anonymous = [col for col in df_complete.columns if "anonymous" not in col.lower()]
            df = df_complete.select(*columns_without_anonymous)
        if dynamo_data.get("mask_columns"):
            masker = DataMasker(dynamo_data.get("mask_columns"))
            df = masker.anonymize(df)
        log_utils.log_info(
            {
                "@STAGE": "DATALAKE BACK",
                "@TABLE_NAME": dynamo_data.get("process_name"),
                "@TASK": "Enriching, cleaning and transforming data",
                "@STATUS": "FINISHED",
                "@LEVEL": "INFO",
            }
        )
        log_utils.log_info(
            {
                "@STAGE": "DATALAKE BACK",
                "@TABLE_NAME": dynamo_data.get("process_name"),
                "@TASK": "Data to be compacted",
                "@STATUS": "RUNNING",
                "@DETAIL": f"Cantidad de registros a compactar {count}",
                "@LEVEL": "INFO",
            }
        )
        parameters = {
            "glue_catalog": datacatalog_name,
            "s3_bucket": (
                f'{args.get("STAGING_BUCKET")}/' f'{dynamo_data.get("staging_prefix")}'
            ),
            "data_bases": dynamo_data.get("data_bases"),
            "table_name": dynamo_data.get("process_name"),
            "primary_keys": dynamo_data.get("primary_keys"),
            "precombine_field": dynamo_data.get("precombine_field"),
            "partition_columns": dynamo_data.get("partition_columns"),
            "type_of_load": dynamo_data.get("type_of_load", "merge"),
            "add_input_missing_columns": dynamo_data.get("add_input_missing_columns"),
            "apply_window_func_bf_write": dynamo_data.get("apply_window_func_bf_write"),
        }
        df.persist()
        IcebergConnector.insert_data(spark, df, parameters)
        if dynamo_data.get("update_metadata") is not None:
            dynamo_data_metadata = AWSDynamoDb.get_data_from_table(
                args.get("DYNAMO_DB_TABLE_NAME_METADATA"),
                "process_id",
                dynamo_data.get("update_metadata"),
                "us-east-1",
            )
            log_utils.log_info(
                {
                    "@STAGE": "DATALAKE BACK",
                    "@TASK": "Updated metadata table",
                    "@TABLE_NAME": dynamo_data.get("process_name"),
                    "@STATUS": "RUNNING",
                    "@DETAIL": (
                        f"The table metadata will be updated {dynamo_data_metadata.get('process_id')}"
                    ),
                    "@LEVEL": "INFO",
                }
            )
            update_metadata_table(dynamo_data_metadata)

    job.commit()
    end = time.time()
    execution_delta = CommonUtils.get_elapsed_seconds(execute_start_time)
    log_utils.log_info(
        {
            "@STAGE": "DATALAKE BACK",
            "@TASK": "End Consulting",
            "@TABLE_NAME": dynamo_data.get("process_name"),
            "@EXECUTE_START_TIME": execute_start_time,
            "@EXECUTE_END_TIME": end,
            "@TOTAL_EXECUTION_TIME": execution_delta,
            "@STATUS": "SUCCESS",
            "@DETAIL": "End of the process to compact the information",
            "@LEVEL": "INFO",
        }
    )


if __name__ == "__main__":
    main()
