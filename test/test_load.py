from moto import mock_aws
import os
import boto3
import pytest
import unittest
from unittest.mock import patch, MagicMock
from src.load import get_connection, get_secrets, lambda_handler, LoadError


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def lambda_event():
    return {"key1": "value1", "key2": "value2", "key3": "value3"}


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


S3_MOCK_BUCKET_NAME = "mock-bucket-1"
S3_MOCK_BUCKET_WRONG_NAME = "wrong-mock-bucket-1"


@pytest.fixture(scope="function")
def s3_bucket(s3):
    s3.create_bucket(
        Bucket=S3_MOCK_BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )



@pytest.fixture(scope="function")
def mock_s3_bucket_env():
    """Mock the environment variable for S3 bucket."""
    os.environ["S3_PROCESS_BUCKET"] = "mock-s3-bucket"
    yield
    del os.environ["S3_PROCESS_BUCKET"]


@pytest.fixture(scope="function")
def lambda_event():
    """Mock Lambda event data."""
    return {
        "tables": [
            "dim_design",
            "dim_staff",
            "dim_location",
            "dim_currency",
            "dim_counterparty",
            "dim_transaction",
            "dim_payment_type",
            "dim_date",
            "fact_sales_order",
            "fact_payment",
            "fact_purchase_order",
        ]
    }


class TestConnection:
    @mock_aws
    def test_get_secrets_from_sm(self):
        sm = boto3.client("secretsmanager", region_name="eu-west-2")

        sm.create_secret(Name="whdb_name", SecretString="wh_db")
        sm.create_secret(Name="whdb_host", SecretString="wh_host")
        sm.create_secret(Name="whdb_user", SecretString="wh_user")
        sm.create_secret(Name="whdb_pass", SecretString="wh_pass")

        response = get_secrets(sm)
        expected_secrets = ["wh_db", "wh_host", "wh_user", "wh_pass"]
        for stored_secret in response.values():
            unittest.TestCase().assertIn(stored_secret, expected_secrets)

    @patch("boto3.client")
    @patch("pg8000.dbapi.connect")
    @patch("src.load.get_secrets")
    def test_database_params_call(self, mock_get_secrets, mock_pg_conn, mock_boto_ct):
        mock_sm_client = MagicMock()
        mock_boto_ct.return_value = mock_sm_client
        mock_get_secrets.return_value = {
            "database": "wh_db",
            "host": "wh_host",
            "user": "wh_user",
            "password": "wh_password",
        }
        mock_pg_conn.return_value = MagicMock()
        get_connection()

        mock_pg_conn.assert_called_once_with(
            database="wh_db",
            host="wh_host",
            user="wh_user",
            password="wh_password",
        )



class TestLambdaHandler:
    @patch("src.load.get_connection")
    @patch("src.load.get_table_df_from_parquet")
    @patch("src.load.store_table_in_wh")
    def test_lambda_handler_success(self, mock_store_table_in_wh, mock_get_table_df, mock_get_connection, lambda_event, mock_s3_bucket_env):
        # Mocking connection and data frame
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_df = MagicMock()
        mock_get_table_df.return_value = mock_df

        # Execute lambda_handler
        result = lambda_handler(lambda_event, None)

        # Assertions
        assert result == {"msg": "Data process successful."}
        assert mock_get_connection.called_once()
        assert mock_get_table_df.called
        assert mock_store_table_in_wh.called

    @patch("src.load.get_connection", side_effect=LoadError("Connection failed"))
    def test_lambda_handler_connection_failure(self, mock_get_connection, lambda_event, mock_s3_bucket_env):
        # Execute lambda_handler
        result = lambda_handler(lambda_event, None)

        # Assertions
        assert result == {
            "msg": "Failed to load data into warehouse",
            "err": "Connection failed",
        }
        mock_get_connection.assert_called_once()

    @patch("src.load.get_table_df_from_parquet", side_effect=LoadError("Failed to load table from S3"))
    @patch("src.load.get_connection")
    def test_lambda_handler_s3_failure(self, mock_get_connection, mock_get_table_df, lambda_event, mock_s3_bucket_env):
        # Mocking connection
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # Execute lambda_handler
        result = lambda_handler(lambda_event, None)

        # Assertions
        assert result == {
            "msg": "Failed to load data into warehouse",
            "err": "Failed to load table from S3",
        }
        mock_get_connection.assert_called_once()
        mock_get_table_df.assert_called_once()

    @patch("src.load.get_table_df_from_parquet")
    @patch("src.load.store_table_in_wh", side_effect=LoadError("Failed to store table in WH"))
    @patch("src.load.get_connection")
    def test_lambda_handler_store_failure(self, mock_get_connection, mock_store_table_in_wh, mock_get_table_df, lambda_event, mock_s3_bucket_env):
        # Mocking connection and data frame
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_df = MagicMock()
        mock_get_table_df.return_value = mock_df

        # Execute lambda_handler
        result = lambda_handler(lambda_event, None)

        # Assertions
        assert result == {
            "msg": "Failed to load data into warehouse",
            "err": "Failed to store table in WH",
        }
        mock_get_connection.assert_called_once()
        mock_get_table_df.assert_called()
        mock_store_table_in_wh.assert_called_once()
