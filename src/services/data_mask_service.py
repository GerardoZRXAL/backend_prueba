from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from loggers.aws_log_service import AWSLogService


class DataMaskerException(Exception):
    pass


class DataMasker:
    """
    Clase para anonimización de datos sensibles en un DataFrame de Spark.
    Aplica reglas de anonimización basadas en expresiones regulares y funciones de enmascaramiento.
    """

    RULES_MAPPING = {
        # Nombres: Nombres, Apellidos, PNR (Record Locator), Ciudad, CP, Colonia
        'names': lambda c: F.when(F.col(c).isNull() | (F.col(c) == ""), None).otherwise(
            F.expr(
                f"""
                        ARRAY_JOIN(
                            TRANSFORM(
                                SPLIT({c}, ' '),
                                word -> CASE
                                        WHEN word = '' THEN ''
                                        ELSE CONCAT(LEFT(word, 1), REPEAT('*', LENGTH(word) - 1))
                                    END
                            ), ' '
                        )
                    """
            )
        ),
        # Correos: oculta todo excepto el dominio
        'email': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NULL OR {col} = '' THEN {col}
                    ELSE CONCAT('******', SUBSTRING({col}, LOCATE('@', {col})))
                END
            """
        ),
        # Correos: oculta lista de correos
        'email_list': lambda col: F.when(
            (F.col(col).isNull()) | (F.col(col) == ""), F.col(col)
        ).otherwise(
            F.expr(
                f"""
            REGEXP_REPLACE({col}, '([^@,]+)(?=@)', '******')
            """
            )
        ),
        # Direcciones
        'address': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NULL OR {col} = '' THEN NULL
                    ELSE CONCAT(SUBSTRING({col}, 0, 5),'**********')
                END
            """
        ),
        # Genero
        'gender': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NULL OR {col} = '' THEN NULL
                    ELSE '******'
                END
            """
        ),
        # Teléfonos: oculta todo excepto los últimos 4 dígitos
        'phone': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NULL OR {col} = '' THEN {col}
                    ELSE CONCAT(REPEAT('*', LENGTH({col}) - 4), SUBSTRING({col}, -4))
                END
            """
        ),
        # Monto
        'amount': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NOT NULL THEN FLOOR(1 + (RAND() * 10))
                    ELSE {col}
                END
            """
        ),
        # Comentario
        'comment': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NULL OR {col} = '' THEN NULL
                    ELSE CONCAT(SUBSTRING({col},0, 5), REPEAT('*', LENGTH({col}) - 5))
                END
            """
        ),
        # Total: reemplaza todo con '*'
        'full_mask': lambda col: F.expr(
            f"""
                CASE
                    WHEN {col} IS NULL OR {col} = '' THEN NULL
                    ELSE REPEAT('*', LENGTH({col}))
                END
            """
        ),
    }

    def __init__(self, rules_config: dict):
        """
        Initializes the class with anonymization rules.

        :param rules_config: Dictionary where keys are column names and values are predefined rules.
        """
        self.rules_config = rules_config

    def anonymize(self, df: DataFrame) -> DataFrame:
        """
        Applies anonymization rules to a copy of each affected column, renaming it with `_anonymous`.

        :param df: Input DataFrame.
        :return: DataFrame with anonymized columns.
        """
        logging = AWSLogService()
        logging.log_info(
            {
                "module": "DataMasker.anonymize",
                "log_output_msg": "**** Init data anonymization ****",
                "status": "Running",
                "@LEVEL": "DEBUG",
            }
        )
        transformations = []
        for column, rule_id in self.rules_config.items():
            if column in df.columns and rule_id in self.RULES_MAPPING:
                # Aplicar la regla correspondiente a la columna
                transformation = self.RULES_MAPPING[rule_id](column)
                # Renombrar la columna resultante con el sufijo "_anonymous"
                transformation = transformation.alias(f"{column}_anonymous")
                # Agregar la transformación a la lista
                transformations.append(transformation)
            else:
                logging.log_error(
                    {
                        "module": "DataMasker.anonymize",
                        "log_output_msg": (
                            f"***** Column '{column}' or rule '{rule_id}' not found in DataFrame *****"
                        ),
                        "status": "Error",
                        "@LEVEL": "ERROR",
                    }
                )
                raise DataMaskerException(
                    f"Column '{column}' or rule '{rule_id}' not found in DataFrame"
                )
        return df.select(*df.columns, *transformations)
