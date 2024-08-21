import boto3
import pytest
import os
import json
import pandas as pd
from botocore.exceptions import ClientError
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


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mock the S3 client."""
    with patch("boto3.client") as mock_s3_client:
        s3_client = mock_s3_client.return_value
        yield s3_client


def test_table_json_to_dataframe_success(s3):
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(MOCK_JSON_DATA).encode("utf-8"))
    }
    df = get_dataframe_from_table_json(S3_BUCKET, TABLE_NAME)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.equals(pd.DataFrame(MOCK_JSON_DATA))
    s3.get_object.assert_called_once_with(
        Bucket=S3_BUCKET, Key=f"latest/2024-08-20 10:14/{TABLE_NAME}.json"
    )


def test_table_json_to_dataframe_client_error(s3):
    s3.get_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        },
        "GetObject",
    )

    with pytest.raises(ProcessError) as exc_info:
        get_dataframe_from_table_json(S3_BUCKET, TABLE_NAME)

    assert "Failed to get table json." in str(exc_info.value)
    s3.get_object.assert_called_once_with(
        Bucket=S3_BUCKET, Key=f"latest/2024-08-20 10:14/{TABLE_NAME}.json"
    )


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
