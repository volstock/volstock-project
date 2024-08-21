import pytest
import os
import json
import pandas as pd
from unittest.mock import patch, MagicMock
from src.process import (
    get_dataframe_from_table_json,
    ProcessError,
    get_dim_staff,
    get_dim_location,
    lambda_handler,
)

S3_BUCKET = "ingest-bucket-20240820100859166800000001"
TABLE_NAME = "mock_table"
MOCK_JSON_DATA = {"column1": ["data1", "data2"], "column2": ["data3", "data4"]}


def test_table_json_to_dataframe_success():
    with patch("src.process.get_dataframe_from_table_json") as mock_get_dataframe:
        mock_get_dataframe.return_value = pd.DataFrame(MOCK_JSON_DATA)
        df = get_dataframe_from_table_json(S3_BUCKET, TABLE_NAME)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert df.equals(pd.DataFrame(MOCK_JSON_DATA))


def test_table_json_to_dataframe_client_error():
    with patch("src.process.get_dataframe_from_table_json") as mock_get_dataframe:
        mock_get_dataframe.side_effect = ProcessError("Failed to get table json.")
        with pytest.raises(ProcessError) as exc_info:
            get_dataframe_from_table_json(S3_BUCKET, TABLE_NAME)

        assert "Failed to get table json." in str(exc_info.value)


@patch("src.process.get_bucket_name")
def test_lambda_handler_env_vars(mock_get_bucket_name):
    mock_get_bucket_name.side_effect = lambda name: f"mock-{name.lower()}"
    lambda_handler({}, {})
    mock_get_bucket_name.assert_any_call("S3_INGEST_BUCKET")
    mock_get_bucket_name.assert_any_call("S3_PROCESS_BUCKET")


@pytest.fixture
def sample_staff():
    return pd.DataFrame(
        {
            "staff_id": [1, 2, 3],
            "first_name": ["John", "Jane", "Doe"],
            "last_name": ["Doe", "Smith", "Jones"],
            "department_id": [10, 20, 30],
            "location": ["New York", "London", "Paris"],
            "email_address": [
                "john@example.com",
                "jane@example.com",
                "doe@example.com",
            ],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"],
        }
    )


@pytest.fixture
def sample_department():
    return pd.DataFrame(
        {
            "department_id": [10, 20, 30],
            "department_name": ["HR", "Finance", "Engineering"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"],
        }
    )


@pytest.fixture
def sample_address():
    return pd.DataFrame(
        {
            "address_id": [100, 200, 300],
            "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
            "city": ["Metropolis", "Gotham", "Star City"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"],
        }
    )


def test_get_dim_staff(sample_staff, sample_department):
    result = get_dim_staff(sample_staff, sample_department)
    expected = pd.DataFrame(
        {
            "staff_id": [1, 2, 3],
            "first_name": ["John", "Jane", "Doe"],
            "last_name": ["Doe", "Smith", "Jones"],
            "department_name": ["HR", "Finance", "Engineering"],
            "location": ["New York", "London", "Paris"],
            "email_address": [
                "john@example.com",
                "jane@example.com",
                "doe@example.com",
            ],
        }
    )

    pd.testing.assert_frame_equal(result, expected)


def test_get_dim_location(sample_address):
    result = get_dim_location(sample_address)
    expected = pd.DataFrame(
        {
            "location_id": [100, 200, 300],
            "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
            "city": ["Metropolis", "Gotham", "Star City"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)
