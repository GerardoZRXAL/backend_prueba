# -*- coding: utf-8 -*-
"""
===================================================================================================
Autor:          Jose Andres Amezcua Garcia
Proyecto:       Datalake Back
Proceso:        spark_service
Descripcion: 	Utileria para ejecutar sentencias en spark.
Modificacion:
                2024-02-25 - Creación
===================================================================================================
"""
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    desc,
    expr,
    from_unixtime,
    row_number,
    struct,
    trim,
    when,
)
from pyspark.sql.types import StringType, ArrayType, StructType, MapType, LongType
from loggers.aws_log_service import AWSLogService


class SparkServiceException(Exception):
    pass


class SparkService:
    @staticmethod
    def read_from_file(
        spark, file_path: str, data_format: str, parameters: dict
    ) -> DataFrameReader:
        """
        This function can read the content of a file that is stored in json, csv, parquet or hudi.
        Args:
            file_path (str): The file's path.
            data_format (str): The format of how is stored the file. E.g. csv, parquet, etc.
            parameters (dict): The different options to read the file.
        Raises:
            SparkServiceException: Raise exception if something fails.
        Returns:
            [DataFrameReader]: A dataframe with the content of the file.
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.read_from_file",
                    "log_output_msg": f"**** Reading the flat file {file_path} ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            return spark.read.format(data_format).options(**parameters).load(file_path)
        except AnalysisException as anlysis_error:
            logging.log_error(
                {
                    "module": "SparkService.read_from_file",
                    "log_output_msg": (
                        f"***** Error to read the path '{file_path}': {anlysis_error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(anlysis_error) from anlysis_error

    @staticmethod
    def save_frame(
        data_frame: DataFrameReader,
        path: str,
        data_format: str,
        mode: str,
        partition_col=None,
    ):
        """
        This function will write a dataframe into a defined path.
        Args:
            data_frame (DataFrameReader): The dataframe that will be written.
            path (str): The path where the file will be stored.
            data_format (str): The data format of how the data will be stored.
            mode (str): Overwrite or Append.
        Raises:
            SparkServiceException: If an error occurred this exception will be raised
        """
        logging = AWSLogService()
        try:
            data_frame.printSchema()
            data_frame.repartition(1).write.format(data_format).mode(mode).partitionBy(
                *partition_col if partition_col is not None else []
            ).save(path)
            logging.log_info(
                {
                    "module": "SparkService.save_frame",
                    "log_output_msg": f"**** The file was saved in {path} ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
        except (AttributeError, Py4JJavaError) as error:
            logging.log_error(
                {
                    "module": "SparkService.read_from_file",
                    "log_output_msg": (
                        f"***** Error to save the df in path '{path}': {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def read_data_query(spark, query_sql: str, temp_view_name=None):
        """
        This function reads the catalog data using queries.
        Args:
            query_sql (str): SQL statement for data query
            temp_view_name (str): Temporary view name for internal execution
        Raises: SparkServiceException
        Returns:
            df (DataFrame): Query result data
        """
        logging = AWSLogService()
        try:
            df = spark.sql(query_sql)
            if temp_view_name:
                df.createOrReplaceTempView(temp_view_name)
            logging.log_info(
                {
                    "module": "SparkService.read_data_query",
                    "log_output_msg": f"**** Running the sql query in spark {query_sql} ****",
                    "status": "Running",
                    "@LEVEL": "DEBUG",
                }
            )
            return df
        except (AttributeError, Py4JJavaError, ParseException) as error:
            logging.log_error(
                {
                    "module": "SparkService.read_data_query",
                    "log_output_msg": (
                        f"***** Error running the sql query in spark '{query_sql}': {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def add_dynamic_columns(data_frame, map_cols):
        """
        This method adds the mapped columns to the df with dynamic type handling.
        Args:
            data_frame (dataframe): The frame where the column will be added
            map_cols (dict): The dict with mapped columns.
        Returns:
            [dataframe]: The dataframe with the column added
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.add_dynamic_columns",
                    "key": "***** Starts method for adding mapped columns *****",
                    "@LEVEL": "DEBUG",
                }
            )

            for key, value in map_cols.items():
                # Check if the column exists and its type
                if key in data_frame.columns:
                    column_type = data_frame.schema[key].dataType

                    # Handle ArrayType columns
                    if isinstance(column_type, ArrayType):
                        data_frame = data_frame.withColumn(
                            key,
                            when(col(key).cast("string") == "[]", array()).otherwise(
                                expr(value)
                            ),
                        )

                    # Handle StructType columns
                    elif isinstance(column_type, StructType):
                        data_frame = data_frame.withColumn(
                            key,
                            when(col(key).cast("string") == "{}", struct()).otherwise(
                                expr(value)
                            ),
                        )

                    # For other types, use standard column addition
                    else:
                        data_frame = data_frame.withColumn(key, expr(value))

                # If column doesn't exist, add it normally
                else:
                    data_frame = data_frame.withColumn(key, expr(value))

            logging.log_info(
                {
                    "module": "SparkService.add_dynamic_columns",
                    "log_output_msg": "***** Adding the new columns *****",
                    "col_value": map_cols,
                    "@LEVEL": "DEBUG",
                }
            )
            return data_frame

        except (ParseException, AnalysisException) as error:
            logging.log_error(
                {
                    "module": "SparkService.add_dynamic_columns",
                    "log_output_msg": f"***** Error adding the columns: {error} *****",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def select_columns(data_frame, columns):
        """
        This method selects the columns from the df.
        Args:
            data_frame (dataframe): The frame where the column will be selected
            columns (list): The list with the selected columns
        Returns:
            [dataframe]: The dataframe with the column selected
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.select_columns",
                    "log_output_msg": "Starts method for selecting columns",
                    "@LEVEL": "DEBUG",
                }
            )
            return data_frame.select(columns)
        except (ParseException, AnalysisException) as error:
            logging.log_error(
                {
                    "module": "SparkService.select_columns",
                    "log_output_msg": (
                        f"***** Error select the columns {columns}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def renamed_columns(df, map_cols):
        """
        This method renamed columns to the df.
        Args:
            df (dataframe): The frame where the column will be added
            map_cols (dict): The dict with mapped colums that will be renamed.
        Returns:
            [dataframe]: The dataframe with the column added
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.renamed_columns",
                    "log_output_msg": "Starts method for renamed columns",
                    "@LEVEL": "DEBUG",
                }
            )
            for key in map_cols:
                logging.log_info(
                    {
                        "module": "SparkService.renamed_columns",
                        "log_output_msg": "Renamed the column",
                        "col_name": key,
                        "col_value": map_cols.get(key),
                        "@LEVEL": "DEBUG",
                    }
                )
                df = df.withColumnRenamed(key, map_cols.get(key))
            df.printSchema()
            return df
        except (ParseException, AnalysisException, AttributeError) as error:
            logging.log_error(
                {
                    "module": "SparkService.select_columns",
                    "log_output_msg": (
                        f"***** Error renamed the columns {map_cols}: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def remove_columns(data_frame, columns_to_remove):
        """
        This method removes columns from the DataFrame.
        Args:
            data_frame (dataframe): DataFrame to be applied the function.
            columns_to_remove (list): A list with the names of the columns to be removed from the DataFrame.
        Returns:
            [dataframe]: dataframe with columns removed
        """
        log_utils = AWSLogService()
        log_utils.log_info(
            {
                "module": "SparkService.remove_columns",
                "log_output_msg": "Columns before removing",
                "value": data_frame.columns,
                "@LEVEL": "INFO",
            }
        )
        for column in columns_to_remove:
            data_frame = data_frame.drop(column)
        log_utils.log_info(
            {
                "module": "SparkService.remove_columns",
                "log_output_msg": "Columns after removing",
                "value": data_frame.columns,
                "@LEVEL": "INFO",
            }
        )
        return data_frame

    @staticmethod
    def trim_string_columns(df):
        """
        This method trims all string columns of the DataFrame.
        Properly handles column names containing dots (.) by escaping them with backticks.
        Args:
            df (dataframe): The frame where that will be trim
        Returns:
            [dataframe]: The dataframe with trimmed columns
        """
        log_utils = AWSLogService()
        log_utils.log_info(
            {
                "module": "SparkService.trim_string_columns",
                "log_output_msg": "Initiates trimming of string columns",
                "@LEVEL": "INFO",
            }
        )
        trimmed_columns = [
            # Apply trim only to StringType columns
            (
                trim(col(f"`{c.name}`")).alias(c.name)
                if isinstance(c.dataType, StringType)
                else col(f"`{c.name}`")
            )  # Directly select non-string columns
            for c in df.schema.fields
        ]
        return df.select(*trimmed_columns)

    @staticmethod
    def cast_column_type(df, column, data_type):
        """
        Function to cast a columns a new type.
        Args:
            df (dataframe): A dataframe for cast the column.
            column (str): The name of the column.
            data_type (str): The type for cast the column.
        Returns:
            [dataframe] A dataframe with its column cast.
        """
        if "numeric" in data_type:
            data_type = data_type.replace("numeric", "decimal")
            df = df.withColumn(f"{column}", col(f"{column}").cast(f"{data_type}"))
        elif "timestamp" in data_type:
            df = df.withColumn(
                f"{column}",
                from_unixtime(col(f"{column}") / 1000, "yyyy-MM-dd HH:mm:ss").cast(
                    "timestamp"
                ),
            )
        elif "date" in data_type:
            df = df.withColumn(
                f"{column}",
                from_unixtime(col(f"{column}") / 1000, "yyyy-MM-dd").cast("date"),
            )
        elif "integer" in data_type:
            df = df.withColumn(f"{column}", col(f"{column}").cast("integer"))
        elif "bigint" in data_type:
            df = df.withColumn(f"{column}", col(f"{column}").cast("long"))
        elif "decimal" in data_type:
            df = df.withColumn(f"{column}", col(f"{column}").cast(f"{data_type}"))
        elif "float" in data_type:
            df = df.withColumn(f"{column}", col(f"{column}").cast("float"))
        elif "double" in data_type:
            df = df.withColumn(f"{column}", col(f"{column}").cast("double"))
        return df


    @staticmethod
    def merge_duplicate_columns(df):
        """
        Merges duplicate columns that only differ in case sensitivity in a PySpark DataFrame.
        The merged columns use coalesce to prioritize the first non-null value found.
        Args:
            df (DataFrame): The input DataFrame.
        Returns:
            DataFrame: The DataFrame with merged columns.
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.merge_duplicate_columns",
                    "log_output_msg": "Initiates the method for merging duplicate columns",
                    "@LEVEL": "DEBUG",
                }
            )
            # Obtener columnas únicas ignorando mayúsculas/minúsculas
            column_mapping = {}
            for unic_col in df.columns:
                col_lower = unic_col.lower()
                if col_lower not in column_mapping:
                    column_mapping[col_lower] = [unic_col]
                else:
                    column_mapping[col_lower].append(unic_col)
            # logging.log_info(
            #     {
            #         "module": "SparkService.merge_duplicate_columns",
            #         "log_output_msg": f"Unic cols {column_mapping}",
            #         "@LEVEL": "DEBUG",
            #     }
            # )
            # Combinar columnas duplicadas
            for col_group in column_mapping.values():
                # En la lista se tienen las columnas duplicadas
                if len(col_group) > 1:
                    # logging.log_info(
                    #     {
                    #         "module": "SparkService.merge_duplicate_columns",
                    #         "log_output_msg": f"df with duplicated columns {col_group}",
                    #         "@LEVEL": "DEBUG",
                    #     }
                    # )
                    # Combinar todas las columnas duplicadas en una, usando coalesce
                    primary_col = col_group[
                        0
                    ]  # Usar la primera columna como nombre principal
                    coalesce_cols = [F.col(c) for c in col_group]
                    df = df.withColumn(primary_col, coalesce(*coalesce_cols))

                    # Eliminar columnas duplicadas, excepto la principal
                    for col_to_drop in col_group[1:]:
                        df = df.drop(col_to_drop)
            logging.log_info(
                {
                    "module": "SparkService.merge_duplicate_columns",
                    "log_output_msg": "Completed method for merging duplicate columns",
                    "@LEVEL": "DEBUG",
                }
            )
            return df
        except (AttributeError, AnalysisException) as error:
            logging.log_error(
                {
                    "module": "SparkService.merge_duplicate_columns",
                    "log_output_msg": (
                        f"***** Error merging duplicate columns: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def lower_columns(df):
        """
        This method lower all columns of the dataframe.
        Args:
            df (dataframe): The frame where that will be lower
        Returns:
            [dataframe]: The dataframe with lower columns
        """
        logging = AWSLogService()
        logging.log_info(
            {
                "module": "SparkService.lower_columns",
                "log_output_msg": "Initiates the method to lowercase all DataFrame columns",
                "@LEVEL": "DEBUG",
            }
        )
        columns = []
        for column_name in df.columns:
            # columns.append(col(column_name).alias(column_name.lower()))
            escaped_column_name = (
                f"`{column_name}`" if "." in column_name else column_name
            )
            columns.append(col(escaped_column_name).alias(column_name.lower()))
        return df.select(*columns)

    @staticmethod
    def replace_spaces_with_underscores(df):
        """
        Replaces spaces in column names with underscores (_) in a PySpark DataFrame.
        Args:
            df (DataFrame): A native PySpark DataFrame whose column names need to be adjusted.
        Returns:
            DataFrame: A new DataFrame with updated column names where spaces are replaced by underscores.
        """
        logging = AWSLogService()
        logging.log_info(
            {
                "module": "SparkService.replace_spaces_with_underscores",
                "log_output_msg": "Initiates the method to lowercase all DataFrame columns",
                "@LEVEL": "DEBUG",
            }
        )
        # Crear una lista de expresiones para renombrar columnas
        renamed_columns = [
            f"`{col}` as `{col.replace(' ', '_')}`" for col in df.columns
        ]

        # Aplicar el renombrado usando selectExpr
        return df.selectExpr(*renamed_columns)

    @staticmethod
    def normalize_column_types(df):
        """
        Normalizes data types for duplicate columns in a PySpark DataFrame.
        Prioritizes any data type over StringType when conflicts arise.
        Args:
            df (DataFrame): The input DataFrame.
        Returns:
            DataFrame: The DataFrame with normalized column types.
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.normalize_column_types",
                    "log_output_msg": "Initiates the normalization of column types",
                    "@LEVEL": "DEBUG",
                }
            )
            # Group columns ignoring case sensitivity
            column_mapping = {}
            for col_name in df.columns:
                col_lower = col_name.lower()
                column_mapping.setdefault(col_lower, []).append(col_name)

            # Generate transformations based on simple priority logic
            transformations = {}
            for col_group in column_mapping.values():
                if len(col_group) > 1:  # Process only duplicate columns
                    # Get data types of all duplicate columns
                    data_types = [
                        df.schema[col_name].dataType for col_name in col_group
                    ]

                    # Check if ArrayType is present
                    array_types = [t for t in data_types if isinstance(t, ArrayType)]
                    if array_types:
                        prioritized_type = ArrayType(StringType())
                    else:
                        # Default to the first non-StringType if available
                        non_string_types = [
                            t for t in data_types if not isinstance(t, StringType)
                        ]
                        # prioritized_type = non_string_types[0] if non_string_types else StringType()
                        if non_string_types:  # If there's at least one non-string type
                            prioritized_type = non_string_types[0]
                        else:  # If all are strings, keep StringType
                            prioritized_type = StringType()

                    # # Check if there's a non-string type
                    # non_string_types = [t for t in data_types if not isinstance(t, StringType)]

                    # if non_string_types:  # If there's at least one non-string type
                    #     prioritized_type = non_string_types[0]
                    # else:  # If all are strings, keep StringType
                    #     prioritized_type = StringType()

                    # Apply transformations to align with the prioritized type
                    for col_name, col_type in zip(col_group, data_types):
                        if col_type != prioritized_type:
                            if isinstance(prioritized_type, ArrayType):
                                # transformations[col_name] = F.array(col(col_name))
                                transformations[col_name] = F.split(col(col_name), ",")
                            else:
                                transformations[col_name] = col(col_name).cast(
                                    prioritized_type.simpleString()
                                )

            # Apply transformations in a single pass
            for col_name, transformation in transformations.items():
                df = df.withColumn(col_name, transformation)

            logging.log_info(
                {
                    "module": "SparkService.normalize_column_types",
                    "log_output_msg": "Completed method for normalizing column types",
                    "@LEVEL": "DEBUG",
                }
            )
            return df
        except (ParseException, AnalysisException, AttributeError) as error:
            logging.log_error(
                {
                    "module": "SparkService.normalize_column_types",
                    "log_output_msg": (
                        f"***** Error normalizing columns: {error} *****"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def add_missing_schema_columns(
        df: DataFrame, required_schema: StructType
    ) -> DataFrame:
        """
        Adds missing columns to a DataFrame based on a required schema.

        This function checks the columns of the given DataFrame against the required schema.
        If any columns from the required schema are missing in the DataFrame, they are added
        with null values and the correct data type. The resulting DataFrame will have columns
        ordered to match the required schema.

        Args:
            df (DataFrame): The input DataFrame to which missing columns will be added.
            required_schema (StructType): The schema defining the required columns and their data types.

        Returns:
            DataFrame: A new DataFrame with all required columns, including any that were missing
                    from the original DataFrame, ordered to match the required schema.
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.add_missing_schema_columns",
                    "log_output_msg": "Initiates the method for adding missing schema columns",
                    "@LEVEL": "DEBUG",
                }
            )
            current_columns = set(df.columns)
            required_columns = {field.name: field.dataType for field in required_schema}

            # Generar expresiones para todas las columnas
            select_exprs = [
                (
                    F.col(col_name)
                    if col_name in current_columns
                    else F.lit(None).cast(col_type).alias(col_name)
                )
                for col_name, col_type in required_columns.items()
            ]
            # Agregar columnas adicionales de raw.
            additional_columns = [
                F.col(col_name)
                for col_name in df.columns
                if col_name not in required_columns
            ]

            # Combinar columnas requeridas y adicionales
            select_exprs.extend(additional_columns)

            logging.log_info(
                {
                    "module": "SparkService.add_missing_schema_columns",
                    "log_output_msg": "Completed method for adding missing schema columns",
                    "dateil": "Columns added",
                    "@LEVEL": "DEBUG",
                }
            )
            df = df.select(*select_exprs)
            return df
        except (ParseException, AnalysisException, AttributeError) as error:
            logging.log_error(
                {
                    "module": "SparkService.add_missing_schema_columns",
                    "log_output_msg": (
                        f"***** Error add missing schema columns: {error}"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def cast_columns_to_match_schema(
        raw_df: DataFrame, stg_schema: StructType
    ) -> DataFrame:
        """
        Casts the columns of a raw DataFrame to match the schema of a target DataFrame.
        This function iterates over the columns of the raw DataFrame and casts them to the corresponding
        types specified in the staging schema. If a column in the raw DataFrame is not present in the
        staging schema, it is retained without any changes. Special handling is provided for columns
        that need more complex conversions, such as converting a string to an array of strings.
        Args:
            raw_df (DataFrame): The raw DataFrame whose columns need to be cast.
            stg_schema (StructType): The schema of the staging DataFrame, which provides the target column types.
        Returns:
            DataFrame: A new DataFrame with columns cast to the types specified in the staging schema.
        Raises:
            SparkServiceException: If there is an error during the casting process, an exception is logged and raised.
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.cast_columns_to_match_schema",
                    "log_output_msg": "Initiates cast columns to match schema",
                    "@LEVEL": "DEBUG",
                }
            )
            # Obtener los tipos de columna del esquema STG
            stg_column_types = {field.name: field.dataType for field in stg_schema}
            raw_column_types = {field.name: field.dataType for field in raw_df.schema}

            casted_columns = []
            for column_name in raw_df.columns:
                if column_name in stg_column_types:
                    target_type = stg_column_types[column_name]
                    current_type = raw_column_types[column_name]

                    if isinstance(target_type, ArrayType) and isinstance(
                        target_type.elementType, StringType
                    ):
                        # Convertir de string a array<string>
                        # casted_columns.append(
                        #     F.from_json(F.col(column_name), ArrayType(StringType())).alias(column_name)
                        # )
                        if isinstance(current_type, ArrayType):
                            casted_columns.append(
                                F.col(column_name).cast(target_type).alias(column_name)
                            )
                        # Si la columna es un string JSON, convertir a array
                        elif isinstance(current_type, StringType):
                            casted_columns.append(
                                F.from_json(
                                    F.col(column_name), ArrayType(StringType())
                                ).alias(column_name)
                            )
                        # Si la columna es un string simple, convertir a array de un elemento
                        else:
                            casted_columns.append(
                                F.array(F.col(column_name).cast(StringType())).alias(
                                    column_name
                                )
                            )
                    else:
                        # Castear para otros tipos
                        casted_columns.append(
                            F.col(column_name).cast(target_type).alias(column_name)
                        )
                else:
                    # Conservar la columna sin cambios si no está en el esquema stg.
                    casted_columns.append(F.col(column_name))

            return raw_df.select(*casted_columns)

        except (ParseException, AnalysisException, AttributeError, TypeError) as error:
            logging.log_error(
                {
                    "module": "SparkService.cast_columns_to_match_schema",
                    "log_output_msg": (
                        f"***** Error cast columns to match schema: {error}"
                    ),
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def apply_window_func_to_df(df, keys, column_to_order):
        """
        This function allows you to clean, organize, and delete
        duplicated records from the dataframe. It applies a window function
        to group based on the key and organizes it based on another column (e.g., date).
        It returns a clean dataframe with the most recent information in the key column
        Args:
            df (dataframe): Dataframe object
            keys (list): The list of keys to perform the compactation
            column_to_order (str): The column to take the newest value
        Raises:
            FrameUtilsException: Exception thrown if an error occurred
        Returns:
            [dataframe]: The frame with newest data
        """
        try:
            logging = AWSLogService()
            logging.log_info(
                {
                    "key": "Apply window func to df",
                    "keys_to_compact": keys,
                    "columns_to_order": column_to_order,
                    "@LEVEL": "DEBUG",
                }
            )

            window = Window.partitionBy(keys).orderBy(desc(column_to_order))
            df = df.withColumn("_row_number", row_number().over(window))
            df = df.where(df["_row_number"] == 1).drop("_row_number")
        except AnalysisException as error:
            logging = AWSLogService()
            logging.log_error(
                "An error ocurred trying apply window funtion to df with these options "
                f"keys: {keys} and column_to_order: {column_to_order}"
            )
            raise SparkServiceException(error) from error
        return df

    @staticmethod
    def flatten_api_nested_columns(df, separator="_", lower_columns=True, prefix=""):
        """
        Enhanced method to flatten nested columns, properly handling complex structures
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.flatten_nested_columns",
                    "log_output_msg": "Starts method to flatten API nested columns",
                    "@LEVEL": "DEBUG",
                }
            )

            fields = df.schema.fields
            flat_cols = []
            used_names = set()

            for field in fields:
                name = field.name
                escaped_name = f"`{name}`" if "." in name else name

                # Handle nested structures (StructType)
                if isinstance(field.dataType, StructType):
                    for nested_field in field.dataType.fields:
                        nested_name = nested_field.name
                        nested_column_name = (
                            f"{prefix}{name}{separator}{nested_name}"
                            if prefix
                            else f"{name}{separator}{nested_name}"
                        )

                        if lower_columns:
                            nested_column_name = nested_column_name.lower()

                        if nested_column_name in used_names:
                            nested_column_name = f"{nested_column_name}_renamed"
                        used_names.add(nested_column_name)

                        flat_cols.append(
                            F.col(f"{escaped_name}.{nested_name}").alias(
                                nested_column_name
                            )
                        )

                # Handle arrays
                elif isinstance(field.dataType, ArrayType):
                    column_name = f"{prefix}{name}" if prefix else name

                    if lower_columns:
                        column_name = column_name.lower()

                    if column_name in used_names:
                        column_name = f"{column_name}_renamed"
                    used_names.add(column_name)

                    # Cast array<long> to array<bigint>
                    if isinstance(field.dataType.elementType, LongType):
                        flat_cols.append(
                            F.col(escaped_name).cast("array<bigint>").alias(column_name)
                        )
                    else:
                        flat_cols.append(F.col(escaped_name).alias(column_name))

                # Handle non-struct columns
                else:
                    column_name = f"{prefix}{name}" if prefix else name

                    if lower_columns:
                        column_name = column_name.lower()

                    if column_name in used_names:
                        column_name = f"{column_name}_renamed"
                    used_names.add(column_name)

                    flat_cols.append(F.col(escaped_name).alias(column_name))

            # Select the flattened columns
            return df.select(flat_cols)

        except Exception as e:
            logging.log_error(
                {
                    "module": "SparkService.flatten_nested_columns",
                    "log_output_msg": f"Error: {str(e)}",
                    "@LEVEL": "ERROR",
                }
            )
            raise

    @staticmethod
    def flatten_nested_columns(df, lower_columns=True, prefix="", flattening_depth=2):
        """
        Flattens a Spark DataFrame that contains nested JSON structures up to the specified depth.
        Optimized version for better performance while maintaining functionality.
        """
        logging = AWSLogService()
        # Ensure flattening_depth is an int
        try:
            flattening_depth = int(flattening_depth)
        except (ValueError, TypeError):
            flattening_depth = 2
        try:
            logging.log_info(
                {
                    "module": "SparkService.flatten_nested_columns",
                    "log_output_msg": f"Starting flatten_nested_columns with depth={flattening_depth}",
                    "@LEVEL": "INFO",
                }
            )
            # Get all columns and their data types
            fields = df.schema.fields
            flat_cols = []
            used_names = set()
            # Optimización: Analizar y procesar los campos en un solo recorrido
            for field in fields:
                name = field.name
                data_type = field.dataType
                escaped_name = f"`{name}`" if "." in name else name
                # Process struct fields (level 1)
                if isinstance(data_type, StructType):
                    # Process each field in the struct (level 2)
                    for nested_field in data_type.fields:
                        nested_name = nested_field.name
                        nested_type = nested_field.dataType
                        escaped_nested_name = f"`{nested_name}`" if "." in nested_name else nested_name
                        # Process level 3 if depth is at least 3 AND the field is a struct
                        if flattening_depth >= 3 and isinstance(nested_type, StructType):
                            # Process each field in level 3
                            for third_level_field in nested_type.fields:
                                third_level_name = third_level_field.name
                                escaped_third_level = f"`{third_level_name}`" if "." in third_level_name else third_level_name
                                if lower_columns:
                                    column_name = f"{prefix}{name}_{nested_name}_{third_level_name}".lower()
                                else:
                                    column_name = f"{prefix}{name}_{nested_name}_{third_level_name}"
                                if column_name in used_names:
                                    column_name = f"{column_name}_renamed"
                                used_names.add(column_name)
                                # Add the level 3 column
                                col_path = f"{escaped_name}.{escaped_nested_name}.{escaped_third_level}"
                                flat_cols.append(F.col(col_path).alias(column_name))
                        else:
                            # Process as normal level 2 column
                            if lower_columns:
                                column_name = f"{prefix}{name}_{nested_name}".lower()
                            else:
                                column_name = f"{prefix}{name}_{nested_name}"
                            if column_name in used_names:
                                column_name = f"{column_name}_renamed"
                            used_names.add(column_name)
                            # Add the level 2 column
                            col_path = f"{escaped_name}.{escaped_nested_name}"
                            flat_cols.append(F.col(col_path).alias(column_name))
                # Handle array types and primitive types - simplificar para mejor rendimiento
                else:
                    if lower_columns:
                        column_name = f"{prefix}{name}".lower()
                    else:
                        column_name = f"{prefix}{name}"
                    if column_name in used_names:
                        column_name = f"{column_name}_renamed"
                    used_names.add(column_name)
                    flat_cols.append(F.col(escaped_name).alias(column_name))
            logging.log_info(
                {
                    "module": "SparkService.flatten_nested_columns",
                    "log_output_msg": f"Completed flattening with depth={flattening_depth}, columns={len(flat_cols)}",
                    "@LEVEL": "INFO",
                }
            )
            # Realizar la selección en una sola operación
            return df.select(flat_cols)
        except Exception as error:
            logging.log_error(
                {
                    "module": "SparkService.flatten_nested_columns",
                    "log_output_msg": f"Error flattening columns: {str(error)}",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error

    @staticmethod
    def process_json_string_fields(df):
        """
        Procesa campos String que contienen valores JSON en formato {key=value}
        y los convierte a columnas individuales.
        Args:
            df: DataFrame con campos string que contienen JSON
        Returns:
            DataFrame con campos JSON procesados
        """
        logging = AWSLogService()
        logging.log_info(
            {
                "module": "SparkService.process_json_string_fields",
                "log_output_msg": "Iniciando procesamiento de campos JSON en string",
                "@LEVEL": "INFO",
            }
        )
        try:
            # Lista de columnas que sabemos contienen JSON en formato string
            json_string_fields = [
                "execution_group",
                "execution_sort"
            ]
            result_df = df
            fields_to_drop = []
            for field_name in json_string_fields:
                if field_name in df.columns:
                    logging.log_info(
                        {
                            "module": "SparkService.process_json_string_fields",
                            "log_output_msg": f"Procesando campo string con formato JSON: {field_name}",
                            "@LEVEL": "DEBUG",
                        }
                    )
                    # Verificar si la columna contiene datos en formato {key=value}
                    sample_data = df.select(field_name).limit(1).collect()
                    if not sample_data or sample_data[0][0] is None:
                        continue
                    sample_value = str(sample_data[0][0])
                    if not (sample_value.startswith('{') and '=' in sample_value):
                        continue
                    # Extraer pares clave=valor usando expresiones regulares en una UDF

                    @F.udf(returnType=MapType(StringType(), StringType()))
                    def extract_key_values(json_str):
                        if not json_str or not isinstance(json_str, str):
                            return {}
                        # Eliminar llaves { }
                        content = json_str.strip().strip('{}')
                        # Extraer pares key=value
                        result = {}
                        for pair in content.split(', '):
                            if '=' in pair:
                                key, value = pair.split('=', 1)
                                result[key] = value
                        return result
                    # Aplicar la UDF y crear un mapa
                    result_df = result_df.withColumn(f"{field_name}_map", extract_key_values(F.col(field_name)))
                    # Ver qué claves están disponibles en el mapa
                    sample_map = result_df.select(f"{field_name}_map").limit(1).collect()[0][0]
                    if not sample_map:
                        continue
                    # Crear columnas individuales para cada clave
                    for key in sample_map.keys():
                        result_df = result_df.withColumn(
                            f"{field_name}_{key}",
                            F.col(f"{field_name}_map")[key]
                        )
                    # Eliminar la columna de mapa temporal
                    result_df = result_df.drop(f"{field_name}_map")
                    # Añadir la columna original a la lista de columnas para eliminar
                    fields_to_drop.append(field_name)
                    logging.log_info(
                        {
                            "module": "SparkService.process_json_string_fields",
                            "log_output_msg": f"Campo {field_name} procesado y expandido en columnas individuales",
                            "@LEVEL": "INFO",
                        }
                    )
            # Eliminar las columnas originales que ya han sido procesadas
            for field_to_drop in fields_to_drop:
                result_df = result_df.drop(field_to_drop)
                logging.log_info(
                    {
                        "module": "SparkService.process_json_string_fields",
                        "log_output_msg": f"Eliminada columna original: {field_to_drop}",
                        "@LEVEL": "DEBUG",
                    }
                )
            return result_df
        except (ValueError, TypeError, AttributeError, AnalysisException) as e:
            # Excepciones específicas que podrían ocurrir durante el procesamiento
            logging.log_error(
                {
                    "module": "SparkService.process_json_string_fields",
                    "log_output_msg": f"Error procesando campos JSON: {str(e)}",
                    "@LEVEL": "ERROR",
                }
            )
            # Devolver el DataFrame original en caso de error
            return df

    @staticmethod
    def create_profile_data_from_table(dataframe, parameters):
        """
        Creates a profile of the data in the provided dataframe and logs the results.
        Simplified version that properly handles different data types.
        """
        table_name = parameters.get("table_name")
        profile_columns = parameters.get("profile_columns", [])

        logging = AWSLogService()
        logging.log_info(
            {
                "module": "SparkService.create_profile_data_from_table",
                "log_output_msg": f"Starting data profiling for dataframe of table {table_name}",
                "status": "Running",
                "@LEVEL": "INFO",
            }
        )

        # If specific columns are requested for profiling, only profile those
        if not profile_columns:
            profile_columns = dataframe.columns

        profile_results = []

        total_count = dataframe.count()

        for column in profile_columns:
            # Get data type
            column_type = next(
                (
                    field.dataType
                    for field in dataframe.schema.fields
                    if field.name == column
                ),
                None,
            )

            if column_type is None:
                continue

            try:
                # Check for null values
                null_count = dataframe.filter(dataframe[column].isNull()).count()
                has_nulls = null_count > 0

                max_length = None

                # Calculate max length based on data type
                if total_count > 0:
                    data_type_str = str(column_type).lower()

                    # Handle different column types for max_length calculation
                    if "string" in data_type_str:
                        max_length_df = dataframe.select(
                            F.max(F.length(column)).alias("max_length")
                        )
                        max_length = max_length_df.collect()[0]["max_length"]
                    elif "array" in data_type_str:
                        max_length_df = dataframe.select(
                            F.max(F.size(column)).alias("max_length")
                        )
                        max_length = max_length_df.collect()[0]["max_length"]
                    elif "map" in data_type_str:
                        max_length_df = dataframe.select(
                            F.max(F.size(column)).alias("max_length")
                        )
                        max_length = max_length_df.collect()[0]["max_length"]
                    elif "struct" in data_type_str:
                        struct_fields = (
                            column_type.fields if hasattr(column_type, "fields") else []
                        )
                        max_length = len(struct_fields)

                profile_data = {
                    "column_name": column,
                    "data_type": str(column_type),
                    "total_count": total_count,
                    "has_nulls": has_nulls,
                    "max_length": max_length,
                }

                # Logging the profile data
                log_message = (
                    f"Column: {profile_data['column_name']} | "
                    f"Type: {profile_data['data_type']} | "
                    f"Total Count: {profile_data['total_count']} | "
                    f"Has Nulls: {profile_data['has_nulls']}"
                )

                if profile_data["max_length"] is not None:
                    if (
                        "array" in str(column_type).lower()
                        or "map" in str(column_type).lower()
                    ):
                        log_message += (
                            f" | Array/Map Size: {profile_data['max_length']}"
                        )
                    elif "string" in str(column_type).lower():
                        log_message += f" | Max Length: {profile_data['max_length']}"
                    else:
                        log_message += f" | Size: {profile_data['max_length']}"

                logging.log_info(
                    {
                        "module": "SparkService.create_profile_data_from_table",
                        "log_output_msg": log_message,
                        "status": "Running",
                        "@LEVEL": "INFO",
                    }
                )

                profile_results.append(profile_data)

            except (ValueError, TypeError, AnalysisException, Py4JJavaError) as error:
                logging.log_error(  # Note: Changed to log_error since this is an error condition
                    {
                        "module": "SparkService.create_profile_data_from_table",
                        "log_output_msg": f"Error profiling column {column}: {str(error)}",
                        "status": "Error",
                        "@LEVEL": "ERROR",
                    }
                )

        # Log the complete profile results as a summary
        logging.log_info(
            {
                "module": "SparkService.create_profile_data_from_table",
                "log_output_msg": f"Data Profile Summary for {table_name}:",
                "status": "Summary",
                "@LEVEL": "INFO",
            }
        )

        header = "| Column Name | Data Type | Total Count | Has Nulls | Size/Length |"
        separator = (
            "|-"
            + "-" * 12
            + "|-"
            + "-" * 10
            + "|-"
            + "-" * 12
            + "|-"
            + "-" * 10
            + "|-"
            + "-" * 12
            + "|"
        )

        logging.log_info(
            {
                "module": "SparkService.create_profile_data_from_table",
                "log_output_msg": header,
                "status": "Summary",
                "@LEVEL": "INFO",
            }
        )

        logging.log_info(
            {
                "module": "SparkService.create_profile_data_from_table",
                "log_output_msg": separator,
                "status": "Summary",
                "@LEVEL": "INFO",
            }
        )

        for profile in profile_results:
            max_length_str = (
                str(profile["max_length"])
                if profile["max_length"] is not None
                else "N/A"
            )
            row = f"| {profile['column_name']:<12} | {profile['data_type']:<10} | {profile['total_count']:<12} | {profile['has_nulls']:<10} | {max_length_str:<12} |"

            logging.log_info(
                {
                    "module": "SparkService.create_profile_data_from_table",
                    "log_output_msg": row,
                    "status": "Summary",
                    "@LEVEL": "INFO",
                }
            )

        logging.log_info(
            {
                "module": "SparkService.create_profile_data_from_table",
                "log_output_msg": f"Completed data profiling for {len(profile_results)} columns in table {table_name}",
                "status": "Completed",
                "@LEVEL": "INFO",
            }
        )

    @staticmethod
    def add_available_columns_list(df: DataFrame) -> DataFrame:
        """
        Adds a new column to the DataFrame containing a list of all available column names.
        This allows validation of column existence using the 'expr' function.
        Args:
            df: The input DataFrame.
        Returns:
            DataFrame with the new column added.
        """
        # Get the list of column names
        column_names = df.columns
        # Create a literal array with all column names
        columns_array = array(*[F.lit(col) for col in column_names])
        # Add the new column to the DataFrame
        result_df = df.withColumn("available_columns_list", columns_array)
        return result_df

    @staticmethod
    def validate_existing_columns(dataframe: DataFrame, columns_to_check: dict) -> DataFrame:
        """
        Method to validate the existence of columns within a dataframe and
        if they do not exist, create them with a default value.
        Args:
            dataframe: The input DataFrame.
            columns_to_check: dict with the columns to check and the default value to assign.
        Returns:
            DataFrame with the new columns added.
        """
        logging = AWSLogService()
        try:
            logging.log_info(
                {
                    "module": "SparkService.validate_existing_columns",
                    "key": "***** Starts method for check the existence of the columns *****",
                    "@LEVEL": "DEBUG",
                }
            )
            for key, value in columns_to_check.items():
                if key not in dataframe.columns:
                    logging.log_info(
                        {
                            "module": "SparkService.validate_existing_columns",
                            "log_output_msg": "***** Adding the missing column *****",
                            "col_value": key,
                            "@LEVEL": "DEBUG",
                        }
                    )
                    dataframe = dataframe.withColumn(key, expr(value))
            logging.log_info(
                {
                    "module": "SparkService.validate_existing_columns",
                    "key": "***** Finishing method for check the existence of the columns *****",
                    "@LEVEL": "DEBUG",
                }
            )
            return dataframe
        except (ParseException, AnalysisException) as error:
            logging.log_error(
                {
                    "module": "SparkService.validate_existing_columns",
                    "log_output_msg": f"***** Error Adding the missing column: {error} *****",
                    "status": "Error",
                    "@LEVEL": "ERROR",
                }
            )
            raise SparkServiceException(error) from error
