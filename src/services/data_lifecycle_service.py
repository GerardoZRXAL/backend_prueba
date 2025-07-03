from services.spark_service import SparkService


class DataLifecycleServiceException(Exception):
    """Custom exception for Data Lifecycle Service errors."""


class DataLifecycleService:
    """
    Service responsible for managing the lifecycle of data through archiving operations.

    This class provides functionalities to create and drop archive tables, and to archive data
    using SQL statements. It leverages SparkService for executing the underlying queries.
    """

    @staticmethod
    def drop_archive_table(spark, query_drop_table):
        """
        Drops an archive table using the provided SQL query.

        Args:
            spark: Spark session object used to execute the query
            query_drop_table (str): SQL query string to drop the archive table

        Returns:
            DataFrame: Result of the executed query, likely empty for DROP operations

        Raises:
            DataLifecycleServiceException: If the table drop operation fails
        """
        return SparkService.read_data_query(spark, query_drop_table)

    @staticmethod
    def create_archive_table(spark, create_table_query):
        """
        Creates a new archive table using the provided SQL query.

        Args:
            spark: Spark session object used to execute the query
            create_table_query (str): SQL query string to create the archive table

        Returns:
            DataFrame: Result of the executed query, likely empty for CREATE operations

        Raises:
            DataLifecycleServiceException: If the table creation operation fails
        """
        return SparkService.read_data_query(spark, create_table_query)

    @staticmethod
    def archive_data_with_sql(spark, sql_file):
        """
        Archives data by executing SQL statements provided in the sql_file.

        This method selects the data to be archived and then deletes it from iceberg table.

        Args:
            spark: Spark session object used to execute the query
            sql_file (str): SQL queries string containing the archiving logic

        Returns:
            DataFrame: Result of the executed query containing information about the archived data

        Raises:
            DataLifecycleServiceException: If the archiving operation fails
        """
        return SparkService.read_data_query(spark, sql_file)
