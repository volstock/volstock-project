from datetime import datetime
import json
from moto import mock_aws
import os
import boto3
import pytest
import unittest
from unittest.mock import patch, MagicMock
from src.load import (
    get_connection,
    get_secrets
)


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


class TestConnection:
    @mock_aws
    def test_get_secrets_from_sm(self):
        sm = boto3.client("secretsmanager", region_name="eu-west-2")

        sm.create_secret(Name="wh_name_", SecretString="wh_db")
        sm.create_secret(Name="wh_host_", SecretString="wh_host")
        sm.create_secret(Name="wh_user_", SecretString="wh_user")
        sm.create_secret(Name="wh_pass_", SecretString="wh_pass")

        response = get_secrets(sm)
        expected_secrets = ["wh_db", "wh_host", "wh_user", "wh_pass"]
        for stored_secret in response.values():
            unittest.TestCase().assertIn(stored_secret, expected_secrets)

    @patch("boto3.client")
    @patch("pg8000.native.Connection")
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
