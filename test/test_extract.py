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
    store_date_in_bucket,
    get_date,
    store_table_in_bucket,
    copy_table,
    delete_table,
    update_dict_table,
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

    @patch("src.extract.get_secrets")
    @patch("pg8000.native.Connection")
    @patch("boto3.client")
    def test_db_params_cll_exception(self, mock_boto_ct, mock_pg_conn, mock_get_secrets):
        mock_sm_client = MagicMock()
        mock_boto_ct.return_value = mock_sm_client
        mock_get_secrets.return_value = {
            "database": "test_db",
            "host": "test_host",
            "user": "test_user",
            "password": "test_password",
        }
        mock_pg_conn.side_effect = Exception("Connection failed")

        with pytest.raises(IngestError) as e:
            get_connection()
        assert str(e.value) == "Failed to connect to database. Connection failed"



    @patch.dict(os.environ, {"S3_INGEST_BUCKET": "mock-bucket"})
    @patch("src.extract.get_connection")
    @patch("src.extract.get_table_names")
    @patch("src.extract.is_bucket_empty")
    @patch("src.extract.get_date")
    @patch("boto3.client")
    def test_lambda_handler_success(
        self,
        mock_boto_client,
        mock_get_date,
        mock_is_bucket_empty,
        mock_get_table_names,
        mock_get_connection,
    ):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_get_connection.return_value = MagicMock()
        mock_is_bucket_empty.return_value = False
        mock_get_table_names.return_value = ["table1", "table2"]
        mock_get_date.return_value = "2024-01-01"
        mock_s3_client.copy_object.return_value = {}
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: b'{"created_at": "2024-01-01"}')
        }

        result = lambda_handler({}, {})
        assert result == {"msg": "Ingestion successful"}




    @patch("src.extract.get_connection")
    def test_lambda_handler_missing_env_var(self, mock_get_connection):
        if "S3_INGEST_BUCKET" in os.environ:
            del os.environ["S3_INGEST_BUCKET"]

        result = lambda_handler({}, {})

        assert result == {
            "msg": "Failed to ingest data",
            "err": "Failed to get env bucket name. 'S3_INGEST_BUCKET'"
        }

    @patch.dict(os.environ, {"S3_INGEST_BUCKET": "mock-bucket"})
    @patch("src.extract.get_connection")
    @patch("src.extract.get_table_names")
    @patch("src.extract.is_bucket_empty")
    @patch("src.extract.get_date")
    def test_lambda_handler_with_failing_dependencies(
        self,
        mock_get_date,
        mock_is_bucket_empty,
        mock_get_table_names,
        mock_get_connection,
    ):
        mock_get_connection.side_effect = IngestError("Connection failed")
        mock_is_bucket_empty.return_value = True
        mock_get_table_names.return_value = ["table1", "table2"]
        mock_get_date.return_value = "2024-01-01"

        result = lambda_handler({}, {})

        assert result == {
            "msg": "Failed to ingest data",
            "err": "Connection failed",
        }

    @patch("pg8000.native.Connection")
    def test_update_dict_table_empty_db_response(self, mock_conn, s3, s3_bucket):
        mock_dict_table = {"created_at": ["2024-01-01"]}
        store_table_in_bucket(S3_MOCK_BUCKET_NAME, mock_dict_table, "mock-table", "2024-01-01")

        mock_conn.run.return_value = []
        result = update_dict_table(S3_MOCK_BUCKET_NAME, "mock-table", "2024-01-01", mock_conn)
        assert not result[0]
        assert result[1] == mock_dict_table


class TestGetBucketName:

    @patch.dict("os.environ", {"S3_INGEST_BUCKET": S3_MOCK_BUCKET_NAME})
    def test_get_bucket_name(self):
        assert get_bucket_name() == S3_MOCK_BUCKET_NAME

    def test_get_bucket_name_error(self):
        with pytest.raises(IngestError) as e:
            get_bucket_name()
        assert str(e.value) == "Failed to get env bucket name. 'S3_INGEST_BUCKET'"


class TestIsBucketEmpty:

    def test_is_an_empty_bucket_empty(self, s3_bucket):
        result = is_bucket_empty(S3_MOCK_BUCKET_NAME)
        assert result

    def test_is_bucket_empty_not_empty(self, s3, s3_bucket):
        s3.put_object(
            Body=b"Mock bytes",
            Bucket=S3_MOCK_BUCKET_NAME,
            Key="latest/mock-date/mock-table.json",
        )
        result = is_bucket_empty(S3_MOCK_BUCKET_NAME)
        assert not result

    def test_is_bucket_empty_error(self, s3_bucket):
        with pytest.raises(IngestError) as e:
            is_bucket_empty(S3_MOCK_BUCKET_WRONG_NAME)
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
        )

        object = s3.get_object(
            Bucket=S3_MOCK_BUCKET_NAME,
            Key="latest/mock-date-2/mock-table.json",
        )
        assert object["Body"].read().decode() == "Mock bytes"

    def test_copy_table_error(self, s3, s3_bucket):
        with pytest.raises(IngestError) as e:
            copy_table(S3_MOCK_BUCKET_WRONG_NAME, "s", "d")
        assert (
            str(e.value) == "Failed to copy table. An error occurred "
            "(NoSuchBucket) when calling the CopyObject operation: The specified "
            "bucket does not exist"
        )


class TestDeleteTable:

    def test_delete_table(self, s3, s3_bucket):
        s3.put_object(
            Body=b"Mock bytes",
            Bucket=S3_MOCK_BUCKET_NAME,
            Key="latest/mock-date/mock-table.json",
        )
        delete_table(S3_MOCK_BUCKET_NAME, "latest/mock-date/mock-table.json")
        objects = s3.list_objects_v2(
            Bucket=S3_MOCK_BUCKET_NAME, Prefix="latest/mock-date/"
        )
        assert "Contents" not in objects

    def test_delete_table_error(self, s3, s3_bucket):
        with pytest.raises(IngestError) as e:
            delete_table(
                S3_MOCK_BUCKET_WRONG_NAME,
                "latest/mock-date/mock-table.json",
            )
        assert (
            str(e.value) == "Failed to delete table. An error occurred "
            "(NoSuchBucket) when calling the DeleteObject operation: The specified "
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
            )
        assert (
            str(e.value) == "Failed to store table in bucket. An error occurred "
            "(NoSuchBucket) when calling the PutObject operation: The specified "
            "bucket does not exist"
        )


class TestStoreAndGetDate:

    def test_store_date_in_bucket(self, s3, s3_bucket):
        store_date_in_bucket(S3_MOCK_BUCKET_NAME, "2024-01-01")
        object = s3.get_object(Bucket=S3_MOCK_BUCKET_NAME, Key="latest_date")
        assert object["Body"].read().decode() == "2024-01-01"

    def test_store_date_in_bucket_error(self, s3, s3_bucket):
        with pytest.raises(IngestError) as e:
            store_date_in_bucket(S3_MOCK_BUCKET_WRONG_NAME, "2024-01-01")
        assert (
            str(e.value) == "Failed to store date in bucket. An error occurred "
            "(NoSuchBucket) when calling the PutObject operation: The specified "
            "bucket does not exist"
        )

    def test_get_date(self, s3_bucket):
        store_date_in_bucket(S3_MOCK_BUCKET_NAME, "2024-01-01")
        assert get_date(S3_MOCK_BUCKET_NAME) == "2024-01-01"

    def test_get_date_error(self, s3_bucket):
        with pytest.raises(IngestError) as e:
            get_date(S3_MOCK_BUCKET_NAME)
        assert (
            str(e.value) == "Failed to get date from bucket. An error occurred "
            "(NoSuchKey) when calling the GetObject operation: The specified key "
            "does not exist."
        )


class TestUpdateTable:

    @patch("pg8000.native.Connection")
    def test_update_dict_table_no_update_needed(
        self,
        mock_conn,
        s3,
        s3_bucket,
    ):
        mock_dict_table = {"created_at": ["2024-01-01", "2024-01-01", "2024-01-01"]}
        store_table_in_bucket(
            S3_MOCK_BUCKET_NAME, mock_dict_table, "mock-table", "2024-01-01"
        )
        mock_conn.run.return_value = [
            ["2024-01-01"],
            ["2024-01-01"],
            ["2024-01-01"],
        ]
        result = update_dict_table(
            S3_MOCK_BUCKET_NAME, "mock-table", "2024-01-01", mock_conn
        )
        assert not result[0]
        assert result[1] == mock_dict_table

    @patch("pg8000.native.Connection")
    def test_update_dict_table_update_needed(
        self,
        mock_conn,
        s3,
        s3_bucket,
    ):
        mock_dict_table = {"created_at": ["2024-01-01", "2024-01-01", "2024-01-01"]}
        store_table_in_bucket(
            S3_MOCK_BUCKET_NAME, mock_dict_table, "mock-table", "2024-01-01"
        )

        def side_effect(*args, **kwargs):
            if "length" in kwargs:
                return [["2024-01-02"], ["2024-01-03"]]
            return [
                ["2024-01-01"],
                ["2024-01-01"],
                ["2024-01-01"],
                ["2024-01-02"],
                ["2024-01-03"],
            ]

        mock_conn.run.side_effect = side_effect
        mock_conn.columns = [{"name": "created_at"}]
        result = update_dict_table(
            S3_MOCK_BUCKET_NAME, "mock-table", "2024-01-01", mock_conn
        )
        assert result[0]
        assert result[1] == {
            "created_at": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
            ]
        }


@patch("logging.critical")
@patch("src.extract.get_dict_table")
@patch("src.extract.store_date_in_bucket")
@patch("src.extract.delete_table")
@patch("src.extract.store_table_in_bucket")
@patch("src.extract.update_dict_table")
@patch("src.extract.copy_table")
@patch("src.extract.get_date")
@patch("src.extract.get_table_names")
@patch("src.extract.is_bucket_empty")
@patch("src.extract.get_connection")
@patch.dict("os.environ", {"S3_INGEST_BUCKET": S3_MOCK_BUCKET_NAME})
def test_lambda_handler(
    mock_conn,
    mock_is_bucket_empty,
    mock_get_table_names,
    mock_get_date,
    mock_copy_table,
    mock_update_dict_table,
    mock_store_table_in_bucket,
    mock_delete_table,
    mock_store_date_in_bucket,
    mock_get_dict_table,
    mock_logging,
    aws_credentials,
):
    mock_is_bucket_empty.return_value = True
    assert lambda_handler("", "") == {"msg": "Ingestion successful"}
    mock_is_bucket_empty.return_value = False
    assert lambda_handler("", "") == {"msg": "Ingestion successful"}
    mock_conn.side_effect = IngestError("Connection mocked exception")
    error_response = lambda_handler("", "")
    assert error_response == {
        "msg": "Failed to ingest data",
        "err": "Connection mocked exception",
    }
    mock_logging.assert_called_once_with(error_response)


class TestFormatDate:
    def test_correct_file_formatting_based_on_GMT(self):
        fake_datetime_object = datetime(2024, 8, 19, 9, 30)

        result = format_date(fake_datetime_object)

        assert result == "2024-08-19 09:30"