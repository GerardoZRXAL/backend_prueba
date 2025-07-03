import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from services.data_mask_service import DataMasker, DataMaskerException

class TestDataMasker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("DataMaskerTest") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_anonymize_names(self):
        data = [("John Doe",), ("Jane Smith",), (None,)]
        df = self.spark.createDataFrame(data, ["name"])
        rules_config = {"name": "names"}
        masker = DataMasker(rules_config)
        result_df = masker.anonymize(df)
        result = result_df.select("name_anonymous").collect()
        expected = ["J*** D**", "J*** S****", None]
        self.assertEqual([row.name_anonymous for row in result], expected)

    def test_anonymize_email(self):
        data = [("john.doe@example.com",), ("jane.smith@example.com",), (None,)]
        df = self.spark.createDataFrame(data, ["email"])
        rules_config = {"email": "email"}
        masker = DataMasker(rules_config)
        result_df = masker.anonymize(df)
        result = result_df.select("email_anonymous").collect()
        expected = ["******@example.com", "******@example.com", None]
        self.assertEqual([row.email_anonymous for row in result], expected)

    def test_anonymize_phone(self):
        data = [("1234567890",), ("0987654321",), (None,)]
        df = self.spark.createDataFrame(data, ["phone"])
        rules_config = {"phone": "phone"}
        masker = DataMasker(rules_config)
        result_df = masker.anonymize(df)
        result = result_df.select("phone_anonymous").collect()
        expected = ["******7890", "******4321", None]
        self.assertEqual([row.phone_anonymous for row in result], expected)

    def test_anonymize_address(self):
        data = [("123 Main St",), ("456 Elm St",), (None,)]
        df = self.spark.createDataFrame(data, ["address"])
        rules_config = {"address": "address"}
        masker = DataMasker(rules_config)
        result_df = masker.anonymize(df)
        result = result_df.select("address_anonymous").collect()
        expected = ["123 M**********", "456 E**********", None]
        self.assertEqual([row.address_anonymous for row in result], expected)

    def test_anonymize_full_mask(self):
        data = [("Sensitive Data",), ("Another Data",), (None,)]
        df = self.spark.createDataFrame(data, ["data"])
        rules_config = {"data": "full_mask"}
        masker = DataMasker(rules_config)
        result_df = masker.anonymize(df)
        result = result_df.select("data_anonymous").collect()
        expected = ["**************", "************", None]
        self.assertEqual([row.data_anonymous for row in result], expected)

    def test_anonymize_invalid_column(self):
        data = [("Sensitive Data",)]
        df = self.spark.createDataFrame(data, ["data"])
        rules_config = {"invalid_column": "full_mask"}
        masker = DataMasker(rules_config)
        with self.assertRaises(DataMaskerException):
            masker.anonymize(df)

    def test_anonymize_invalid_rule(self):
        data = [("Sensitive Data",)]
        df = self.spark.createDataFrame(data, ["data"])
        rules_config = {"data": "invalid_rule"}
        masker = DataMasker(rules_config)
        with self.assertRaises(DataMaskerException):
            masker.anonymize(df)


if __name__ == "__main__":
    unittest.main()
