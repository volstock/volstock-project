import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
from src.extract import (
    get_bucket_name,
    get_dict_table,
    get_table_names,
    is_bucket_empty,
    store_table_in_bucket,
    archive_tables,
    IngestError,
    DatabaseError,
)
import json


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
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


S3_MOCK_BUCKET_NAME = "mock-bucket-1"
S3_MOCK_BUCKET_WRONG_NAME = "wrong-mock-bucket-1"


@patch.dict("os.environ", {"S3_INGEST_BUCKET": S3_MOCK_BUCKET_NAME})
def test_get_bucket_name():
    assert get_bucket_name() == S3_MOCK_BUCKET_NAME


def test_get_bucket_name_error():
    with pytest.raises(IngestError) as e:
        get_bucket_name()
    assert str(e.value) == "Failed to get env bucket name. 'S3_INGEST_BUCKET'"


@pytest.fixture(scope="function")
def s3_bucket(s3):
    s3.create_bucket(
        Bucket=S3_MOCK_BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def test_is_bucket_empty_empty(s3, s3_bucket):
    result = is_bucket_empty(S3_MOCK_BUCKET_NAME, s3)
    assert result == (True, [], "latest/")


def test_is_bucket_empty_not_empty(s3, s3_bucket):
    s3.put_object(
        Body=b"Mock bytes",
        Bucket=S3_MOCK_BUCKET_NAME,
        Key="latest/mock-date/mock-table.json",
    )
    result = is_bucket_empty(S3_MOCK_BUCKET_NAME, s3)
    assert result == (False, ["mock-date/mock-table.json"], "latest/")

    s3.put_object(
        Body=b"More mock bytes",
        Bucket=S3_MOCK_BUCKET_NAME,
        Key="latest/mock-date/mock-table-2.json",
    )
    result = is_bucket_empty(S3_MOCK_BUCKET_NAME, s3)
    assert result == (
        False,
        ["mock-date/mock-table-2.json", "mock-date/mock-table.json"],
        "latest/",
    )


def test_is_bucket_empty_error(s3, s3_bucket):
    with pytest.raises(IngestError) as e:
        is_bucket_empty(S3_MOCK_BUCKET_WRONG_NAME, s3)
    assert (
        str(e.value) == "Failed to check if bucket is empty. An error"
        " occurred (NoSuchBucket) when calling the ListObjectsV2 operation:"
        " The specified bucket does not exist"
    )


@patch("pg8000.native.Connection")
def test_get_dict_table(mock_conn):
    mock_conn.run.return_value = [["A", 1], ["B", 2]]
    mock_conn.columns = {"name": "c1"}, {"name": "c2"}
    result = get_dict_table(mock_conn, "mock-db-table-1")
    assert result == {"c1": ["A", "B"], "c2": [1, 2]}


@patch("pg8000.native.Connection")
def test_get_dict_table_error(mock_conn):
    mock_conn.run.side_effect = DatabaseError("Mock DB error")
    with pytest.raises(IngestError) as e:
        get_dict_table(mock_conn, "mock-wrong-db-table")
    assert str(e.value) == "Failed to get table values, Mock DB error"


def test_store_table_in_bucket(s3, s3_bucket):
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


def test_store_table_in_bucket_error(s3, s3_bucket):
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


def test_archive_tables(s3, s3_bucket):
    store_table_in_bucket(
        S3_MOCK_BUCKET_NAME,
        {"c1": [1, 2], "c2": ["A", "B"]},
        "mock-db-table",
        "2024-01-01",
        s3,
    )
    (_, keys, prefix) = is_bucket_empty(S3_MOCK_BUCKET_NAME, s3)
    archive_tables(S3_MOCK_BUCKET_NAME, keys, prefix, s3)
    objects = s3.list_objects_v2(
        Bucket=S3_MOCK_BUCKET_NAME, Prefix=f"{prefix}"
    )
    assert "Contents" not in objects

    objects = s3.list_objects_v2(Bucket=S3_MOCK_BUCKET_NAME, Prefix="archive/")
    assert "Contents" in objects
    keys = [obj["Key"][8:] for obj in objects["Contents"]]
    assert keys == ["2024-01-01/mock-db-table.json"]


def test_get_table_names():
    conn = MagicMock()
    conn.run.return_value = [["t1"], ["t2"], ["t3"], ["t4"]]
    assert get_table_names(conn) == ["t1", "t2", "t3", "t4"]

    conn.run.return_value = [["_t1"], ["t2"], ["_t3"], ["t4"]]
    assert get_table_names(conn) == ["t2", "t4"]


def test_get_table_names_error():
    conn = MagicMock()
    conn.run.side_effect = DatabaseError("Mock DB error")
    with pytest.raises(IngestError) as e:
        get_table_names(conn)
    assert str(e.value) == "Failed to get table names. Mock DB error"
