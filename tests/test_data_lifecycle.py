import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from pyspark.sql import DataFrame
from services.data_lifecycle_service import (
    DataLifecycleService,
    DataLifecycleServiceException,
)


class TestDataLifecycleService:
    @pytest.fixture
    def mock_dataframe(self):
        """Create a mock DataFrame with all necessary methods."""
        df = MagicMock(spec=DataFrame)

        # Setup write chain
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.save = MagicMock()
        df.write = mock_writer

        # Setup column operations
        mock_column = MagicMock()
        mock_column.isNull.return_value = mock_column
        df.__getitem__.return_value = mock_column

        # Setup filter operation
        filtered_df = MagicMock()
        filtered_df.count.return_value = 0
        df.filter.return_value = filtered_df

        # Setup repartition
        df.repartition.return_value = df
        df.coalesce.return_value = df
        df.columns = ["created_at", "data"]

        return df

    @pytest.fixture
    def valid_parameters(self):
        """Fixture for valid parameters."""
        """Fixture for valid parameters."""
        return {
            "table_name": "test_table",
            "date_column": "created_at",
            "cutoff_date_str": "2023-01-01",
            "datacatalog_name": "test_catalog",
            "db_name": "test_db",
            "target_bucket": "XXXXXXXXXXX",
            "partition_columns": ["created_at"],
            "target_partition_size": 104857600,  # 100MB
        }

