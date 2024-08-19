import boto3
from datetime import datetime
import json
from moto import mock_aws
import os
import pytest
import unittest
from unittest.mock import patch, MagicMock
from src.extract import (
    format_date,
    get_secrets,
    get_connection,
    get_bucket_name,
    get_dict_table,
    get_table_names,
    is_bucket_empty,
    store_table_in_bucket,
    copy_table,
    lambda_handler,
    IngestError,
    DatabaseError,
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
    return {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    }

@pytest.fixture(scope="function") #Test if we can remove as never seen this before
def lambda_context():
    class Context:
        function_name = "test_lambda"
        memory_limit_in_mb = 128
        function_version = "1"
        invoked_function_arn = "arn:aws:lambda:aws-region:acct-id:function:test_lambda"
        aws_request_id = "test_request_id"
    return Context()


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

        sm.create_secret(Name="db_name", SecretString="test_db")
        sm.create_secret(Name="db_host", SecretString="test_host")
        sm.create_secret(Name="db_user", SecretString="test_user")
        sm.create_secret(Name="db_pass", SecretString="test_pass")

        response = get_secrets(sm)
        expected_secrets = ["test_db", "test_host", "test_user", "test_pass"]
        for stored_secret in response.values():
            unittest.TestCase().assertIn(stored_secret, expected_secrets)

    @patch("boto3.client")
    @patch("pg8000.native.Connection")
    @patch("src.extract.get_secrets")
    def test_db_params_cll(self, mock_get_secrets, mock_pg_conn, mock_boto_ct):
        mock_sm_client = MagicMock()
        mock_boto_ct.return_value = mock_sm_client
        mock_get_secrets.return_value = {
            "database": "test_db",
            "host": "test_host",
            "user": "test_user",
            "password": "test_password",
        }
        mock_pg_conn.return_value = MagicMock()
        get_connection()

        mock_pg_conn.assert_called_once_with(
            database="test_db",
            host="test_host",
            user="test_user",
            password="test_password",
        )

class TestGetBucketName:

    @patch.dict("os.environ", {"S3_INGEST_BUCKET": S3_MOCK_BUCKET_NAME})
    def test_get_bucket_name(self):
        assert get_bucket_name() == S3_MOCK_BUCKET_NAME

    def test_get_bucket_name_error(self):
        with pytest.raises(IngestError) as e:
            get_bucket_name()
        assert str(e.value) == \
            "Failed to get env bucket name. 'S3_INGEST_BUCKET'"

class TestIsBucketEmpty:

    def test_is_an_empty_bucket_empty(self, s3, s3_bucket):
        result = is_bucket_empty(S3_MOCK_BUCKET_NAME, s3)
        assert result == True

    def test_is_bucket_empty_not_empty(self, s3, s3_bucket):
        s3.put_object(
            Body=b"Mock bytes",
            Bucket=S3_MOCK_BUCKET_NAME,
            Key="latest/mock-date/mock-table.json",
        )
        result = is_bucket_empty(S3_MOCK_BUCKET_NAME, s3)
        assert result == False

    def test_is_bucket_empty_error(self, s3, s3_bucket):
        with pytest.raises(IngestError) as e:
            is_bucket_empty(S3_MOCK_BUCKET_WRONG_NAME, s3)
        assert (
            str(e.value) == "Failed to check if bucket is empty. An error"
            " occurred (NoSuchBucket) when calling the ListObjectsV2 operation:"
            " The specified bucket does not exist"
        )

class TestGetTableNames:

    def test_get_table_names(self):
        conn = MagicMock()
        conn.run.return_value = [["t1"], ["t2"], ["t3"], ["t4"]]
        assert get_table_names(conn) == ["t1", "t2", "t3", "t4"]

        conn.run.return_value = [["_t1"], ["t2"], ["_t3"], ["t4"]]
        assert get_table_names(conn) == ["t2", "t4"]

    def test_get_table_names_error(self):
        conn = MagicMock()
        conn.run.side_effect = DatabaseError("Mock DB error")
        with pytest.raises(IngestError) as e:
            get_table_names(conn)
        assert str(e.value) == "Failed to get table names. Mock DB error"


class TestCopyTable:

    def test_copy_table(self, s3, s3_bucket):
        s3.put_object(
            Body=b"Mock bytes",
            Bucket=S3_MOCK_BUCKET_NAME,
            Key="latest/mock-date-1/mock-table.json",
        )

        copy_table(
            S3_MOCK_BUCKET_NAME,
            "latest/mock-date-1/mock-table.json",
            "latest/mock-date-2/mock-table.json",
            s3,
        )

        object = s3.get_object(
            Bucket=S3_MOCK_BUCKET_NAME, Key="latest/mock-date-2/mock-table.json"
        )
        assert object["Body"].read().decode() == "Mock bytes"


    def test_copy_table_error(self, s3, s3_bucket):
        with pytest.raises(IngestError) as e:
            copy_table(S3_MOCK_BUCKET_WRONG_NAME, "s", "d", s3)
        assert (
            str(e.value) == "Failed to copy table. An error occurred "
            "(NoSuchBucket) when calling the CopyObject operation: The specified "
            "bucket does not exist"
        )

class TestGetDictTable:
    @patch("pg8000.native.Connection")
    def test_get_dict_table(self, mock_conn):
        mock_conn.run.return_value = [["A", 1], ["B", 2]]
        mock_conn.columns = {"name": "c1"}, {"name": "c2"}
        result = get_dict_table(mock_conn, "mock-db-table-1")
        assert result == {"c1": ["A", "B"], "c2": [1, 2]}

    @patch("pg8000.native.Connection")
    def test_get_dict_table_error(self, mock_conn):
        mock_conn.run.side_effect = DatabaseError("Mock DB error")
        with pytest.raises(IngestError) as e:
            get_dict_table(mock_conn, "mock-wrong-db-table")
        assert str(e.value) == "Failed to get table values, Mock DB error"

class TestStoreTableInBucket:
    def test_store_table_in_bucket(self, s3, s3_bucket):
        store_table_in_bucket(
            S3_MOCK_BUCKET_NAME,
            {"c1": [1, 2], "c2": ["A", "B"]},
            "mock-db-table",
            "2024-01-01",
            s3,
        )
        object = s3.get_object(
            Bucket=S3_MOCK_BUCKET_NAME, Key="latest/2024-01-01/mock-db-table.json"
        )
        object_data = json.loads(object["Body"].read().decode("utf-8"))
        assert object_data == {"c1": [1, 2], "c2": ["A", "B"]}

    def test_store_table_in_bucket_error(self, s3, s3_bucket):
        with pytest.raises(IngestError) as e:
            store_table_in_bucket(
                S3_MOCK_BUCKET_WRONG_NAME,
                {"c1": [1, 2], "c2": ["A", "B"]},
                "mock-db-table",
                "2024-01-01",
                s3,
            )
        assert (
            str(e.value) == "Failed to store table in bucket. An error occurred "
            "(NoSuchBucket) when calling the PutObject operation: The specified "
            "bucket does not exist"
        )

class TestLambdasHandler:
    @patch("src.extract.get_bucket_name")
    @patch("src.extract.get_connection")
    @patch("src.extract.is_bucket_empty")
    @patch("src.extract.get_table_names")
    @patch("src.extract.archive_tables")
    @patch("src.extract.get_dict_table")
    @patch("src.extract.store_table_in_bucket")
    def test_lambda_handler_when_given_correct_inputs(
        self,
        mock_store_table_in_bucket,
        mock_get_dict_table,
        mock_archive_tables,
        mock_get_table_names,
        mock_is_bucket_empty,
        mock_get_connection,
        mock_get_bucket_name,
        lambda_event,
        lambda_context
    ):
        mock_get_bucket_name.return_value = "test-bucket"
        mock_get_connection.return_value = MagicMock()
        mock_is_bucket_empty.return_value = (False, ["test-key"], "latest/")
        mock_get_table_names.return_value = ["table1", "table2"]
        mock_get_dict_table.side_effect = [{"col1": [1, 2]}, {"col2": [3, 4]}]
        
        response = lambda_handler(lambda_event, lambda_context)
        
        assert response == {"msg": "Ingestion successfull"}
        

class TestFormatDate:
    def test_correct_file_formatting_based_on_GMT(self):
        fake_datetime_object = datetime(2024, 8, 19, 9, 30)
        
        result = format_date(fake_datetime_object)
        
        assert result == '2024-08-19 09:30'
