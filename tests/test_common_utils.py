import os
import pytest
import time
import unittest
import subprocess
from unittest.mock import patch
from unittest import TestCase
from freezegun import freeze_time
from utils.common_utils import CommonUtils, CommonUtilsException


@patch.dict(os.environ, {"ENV_VAR_TEST": "RED"})
def test_get_environment_variable():
    result = CommonUtils.get_environment_variable("ENV_VAR_TEST")
    assert result == "RED"


def test_get_environment_variable_exception():
    with pytest.raises(CommonUtilsException):
        CommonUtils.get_environment_variable("VAR_ERROR")


def test_load_json_file_exception():
    with pytest.raises(CommonUtilsException):
        CommonUtils.load_json_file("mock/not_exist.json")


def test_load_json_file():
    expected_output = {"foo": "bar"}
    genrated_json = CommonUtils.load_json_file("mock/config_files/json.ini")
    test_case = TestCase()
    TestCase.assertDictEqual(test_case, expected_output, genrated_json)


@pytest.mark.parametrize(
    "file_path, expected_output",
    [
        ("/opt/test/ab.txt", "ab.txt"),
        ("/opt/test.txt/re.xls", "re.xls"),
        ("/opt/test.txt/uy.parquet", "uy.parquet"),
    ],
)
def test_get_file_name(file_path, expected_output):
    output = CommonUtils.get_file_name(file_path)
    assert output == expected_output


def test_get_elapsed_seconds():
    execute_start_time = time.time()
    result = CommonUtils.get_elapsed_seconds(execute_start_time)
    assert result == 0

def test_read_file():
    file = CommonUtils.read_file("mock/config_files/json.ini")

def test_read_file_exception():
    with pytest.raises(CommonUtilsException):
        file = CommonUtils.read_file("mock/config_files/json_2.ini")


@pytest.mark.parametrize(
    "date_format, days_opr, specific_date, timezone, expected_output",
    [
        ("%Y-%m-%d %H:%M:%S", 5, "2024-09-01 19:00:00", "Etc/GMT+5", "2024-09-06 19:00:00"),
        ("%Y-%m-%d", -4, "2024-09-20", "Etc/GMT+5", "2024-09-16"),
        ("%Y-%m-%d %H:%M:%S", 10, "2021-05-20 00:00:00", "UTC", "2021-05-30 00:00:00"),
    ],
)
def test_date_minus_days_with_timezone(date_format, days_opr, specific_date, timezone, expected_output):
    output_generated = CommonUtils.date_minus_days(specific_date, days_opr, date_format, timezone)
    print(output_generated)
    assert output_generated == expected_output

def test_get_spark_sql_type():
    expected_output = "STRING"
    output_generated = CommonUtils.get_spark_sql_type("stringtype")
    assert output_generated == expected_output

@freeze_time("2022-03-01")
def test_calculate_date_from_days():
    expected_output = "02"
    result_date = CommonUtils.calculate_date_from_days(1)
    assert result_date.get("day") == expected_output

def test_parse_date_components():
    expected_output = "01"
    result_date = CommonUtils.parse_date_components("2022-03-01")
    assert result_date.get("day") == expected_output

class TestCommonUtils(unittest.TestCase):
    @patch("os.system")
    def test_kill_yarn_application(self, mock_system):
        CommonUtils.kill_yarn_application()
        mock_system.assert_called_once_with(
            "for x in $(yarn application -list -appStates RUNNING | awk 'NR > 2 { print $1 }'); do yarn application -kill $x; done"
        )

    @patch("os.system", side_effect=OSError("Test error"))
    def test_kill_yarn_application_os_error(self, mock_system):
        CommonUtils.kill_yarn_application()
        mock_system.assert_called_once()

    @patch("subprocess.call", return_value=0)
    def test_delete_path_haddop(self, mock_system):
        path_hdfs = "/test/"
        CommonUtils.delete_path_haddop(path_hdfs)
        mock_system.assert_called_once_with(
            ["hadoop", "fs", "-rm", "-r", "-f", path_hdfs]
        )

    @patch("subprocess.call", return_value=1)
    def test_delete_path_haddop_error(self, mock_system):
        path_hdfs = "/test/"
        with self.assertRaises(CommonUtilsException):
            CommonUtils.delete_path_haddop(path_hdfs)
        mock_system.assert_called_once_with(
            ["hadoop", "fs", "-rm", "-r", "-f", path_hdfs]
        )

    @patch("subprocess.run")
    def test_run_cmd(self, mock_system):
        command_run = "ls test/"
        CommonUtils.run_cmd(command_run)
        mock_system.assert_called_once_with(command_run, timeout=None, check=False)

    @patch("subprocess.run", side_effect=subprocess.TimeoutExpired("Test error", 0))
    def test_run_cmd_error(self, mock_system):
        command_run = "ls test/"
        CommonUtils.run_cmd(command_run, 0)
        mock_system.assert_called_once_with(command_run, timeout=0, check=False)

    @patch("subprocess.call", return_value=0)
    def test_copy_data_hdfs_to_s3(self, mock_system):
        path_hdfs = "/test/"
        path_s3 = "s3://test/"
        CommonUtils.copy_data_hdfs_to_s3(path_hdfs, path_s3)
        mock_system.assert_called_once_with(["hadoop", "distcp", path_hdfs, path_s3])

    @patch("subprocess.call", return_value=1)
    def test_copy_data_hdfs_to_s3_error(self, mock_system):
        path_hdfs = "/test/"
        path_s3 = "s3://test/"
        with self.assertRaises(NameError):
            CommonUtils.copy_data_hdfs_to_s3(path_hdfs, path_s3)
        mock_system.assert_called_once_with(["hadoop", "distcp", path_hdfs, path_s3])
