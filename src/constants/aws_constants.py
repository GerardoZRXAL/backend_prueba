"""
============================================================
Autor:           Jose Andres Amezcua Garcia
Proyecto:        Datalake back
Proceso:         utility / constants
Descripcion:  	 Constantes comunes utilizadas en el proceso.
Modificacion:
                2024-03-06 - Creación
============================================================
"""

# Constants
COL_NULL = "s/i"
FILE_SYSTEM = "s3://"
MAX_ATTEMPT = 3
HDFS_SYSTEM = "hdfs:///"

# Iceberg
QUERY_UPSERT = """
MERGE INTO {0}.{1}.{2} t
USING (SELECT * FROM {2}_raw_data) s
ON {3}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

QUERY_APPEND = """
INSERT INTO {0}.{1}.{2}
SELECT * FROM {2}_raw_data
"""

QUERY_VALIDATE_TABLE = "SHOW TABLES IN {0} LIKE '{1}'"

QUERY_CREATE_TABLE_ICEBERG = "CREATE TABLE {0}.{1}.{2} ({3}) USING iceberg LOCATION 's3://{4}' TBLPROPERTIES ('write.parquet.compression-codec' = 'snappy', 'write.target-file-size-bytes' = '25000000')"

QUERY_CREATE_TABLE_ICEBERG_PARTITION = "CREATE TABLE {0}.{1}.{2} ({3}) USING iceberg LOCATION 's3://{4}' PARTITIONED BY ({5}) TBLPROPERTIES ('write.parquet.compression-codec' = 'snappy', 'write.target-file-size-bytes' = '25000000')"

QUERY_CREATE_TABLE_ICEBERG_PARTITION_AS = """
  CREATE TABLE {0}.{1}.{2}
  USING iceberg
  LOCATION 's3://{3}'
  PARTITIONED BY ({4})
  TBLPROPERTIES ('write.parquet.compression-codec' = 'snappy', 'write.target-file-size-bytes' = '25000000')
  AS {5}
"""

QUERY_DROP_TABLE_CATALOG = "DROP TABLE IF EXISTS {0}.{1}.{2}"

QUERY_DROP_TABLE = "DROP TABLE IF EXISTS {0}.{1}"

QUERY_CREATE_TABLE_PARQUET = """
CREATE TABLE {0}.{1}
 USING parquet
 LOCATION 's3://{2}'
 OPTIONS ('write.format.parquet'='true')
 as {3}
"""

QUERY_GET_TABLE_DATA = "SELECT * FROM {0}.{1}.{2} LIMIT 10"

QUERY_ADD_COLUMN = "ALTER TABLE {0}.{1}.{2} ADD COLUMNS ({3})"

# HDFS Folder
HDFS_RAW_FOLDER = f"{HDFS_SYSTEM}raw_data_sqoop/"
HDFS_STAGING_FOLDER = f"{HDFS_SYSTEM}staging_data_sqoop/"
