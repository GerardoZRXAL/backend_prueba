import pytest
from utils.pandas_utils import PandasUtils, PandasUtilsException


def test_read_excel():
    pandas_args = {
        "io": "mock/config_files/excel_file.xlsx",
        "sheet_name": "Tablas finales",
        "skiprows": 6,
    }
    PandasUtils.read_excel(pandas_args)


def test_read_excel_exception():
    with pytest.raises(PandasUtilsException):
        pandas_args = {
            "io": "mock/config_files/excel_file_error.xlsx",
            "sheet_name": "Tablas finales",
            "skiprows": 6,
        }
        PandasUtils.read_excel(pandas_args)
