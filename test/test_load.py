from moto import mock_aws
import os
import boto3
import pytest
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError  
from pg8000.exceptions import DatabaseError  
from src.load import get_connection, get_secrets, get_bucket_name, LoadError, get_table_df_from_parquet, get_dataframe_values, store_table_in_wh 



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

        sm.create_secret(Name="whdb_name", SecretString="wh_db")
        sm.create_secret(Name="whdb_host", SecretString="wh_host")
        sm.create_secret(Name="whdb_user", SecretString="wh_user")
        sm.create_secret(Name="whdb_pass", SecretString="wh_pass")

        response = get_secrets(sm)
        expected_secrets = ["wh_db", "wh_host", "wh_user", "wh_pass"]
        for stored_secret in response.values():
            unittest.TestCase().assertIn(stored_secret, expected_secrets)

    def test_get_bucket_name_success(self):  
        os.environ['S3_PROCESS_BUCKET'] = S3_MOCK_BUCKET_NAME
        assert get_bucket_name('S3_PROCESS_BUCKET') == S3_MOCK_BUCKET_NAME

    def test_get_bucket_name_failure(self): 
        if 'S3_PROCESS_BUCKET' in os.environ:
            del os.environ['S3_PROCESS_BUCKET']
        with pytest.raises(LoadError) as e:
            get_bucket_name('S3_PROCESS_BUCKET')
        assert "Failed to get env bucket name" in str(e.value)

    @patch("boto3.client")
    def test_get_connection_secrets_failure(self, mock_boto_client):  
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
        )
        mock_boto_client.return_value = mock_client

        with pytest.raises(LoadError) as excinfo:
            get_connection()
        
        assert "Failed to get secrets" in str(excinfo.value)

    @patch("pg8000.dbapi.connect")
    @patch("boto3.client")
    def test_get_connection_db_failure(self, mock_boto_client, mock_pg_connect): 
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_pg_connect.side_effect = DatabaseError("Database connection failed")
        
        with pytest.raises(LoadError) as excinfo:
            get_connection()
        
        assert "Failed to retrieve secrets" not in str(excinfo.value)
        assert "Failed to get connection" in str(excinfo.value)



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

    @patch("pg8000.dbapi.connect")
    @patch("boto3.client")
    def test_get_connection_success(self, mock_boto_client, mock_pg_connect):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_client.get_secret_value.side_effect = [
            {"SecretString": "wh_db"},
            {"SecretString": "wh_host"},
            {"SecretString": "wh_user"},
            {"SecretString": "wh_pass"},
        ]

        mock_pg_connect.return_value = MagicMock()

        conn = get_connection()

        mock_pg_connect.assert_called_once_with(
            database="wh_db",
            host="wh_host",
            user="wh_user",
            password="wh_pass",
        )
        assert conn is not None

    @patch("boto3.client")
    def test_get_connection_secrets_failure(self, mock_boto_client):
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
        )
        mock_boto_client.return_value = mock_client

        with pytest.raises(LoadError) as excinfo:
            get_connection()

        assert "Failed to get secrets" in str(excinfo.value)

    @patch("boto3.client")
    def test_get_table_df_from_parquet_success(self, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.download_fileobj.side_effect = lambda Bucket, Key, Fileobj: Fileobj.write(b"parquet_data")
    
        with patch("pandas.read_parquet") as mock_read_parquet:
            mock_read_parquet.return_value = pd.DataFrame({"col1": pd.Series([1, 2, 3], dtype="Int64")})
            result = get_table_df_from_parquet("mock_bucket", "mock_parquet")
            expected_df = pd.DataFrame({"col1": pd.Series([1, 2, 3], dtype="Int64")})
            pd.testing.assert_frame_equal(result, expected_df)

    def get_dataframe_values(df, conn, table_name):
        try:
            whdb_row_count = conn.execute(f'SELECT COUNT(*) FROM {table_name}')[0]
            s3_row_count = len(df)
            row_diff = s3_row_count - whdb_row_count

            print(f"whdb_row_count: {whdb_row_count}, s3_row_count: {s3_row_count}, row_diff: {row_diff}")

            if row_diff > 0:
                return df.tail(row_diff).values.tolist()
            else:
                return []  
        except Exception as e:
            raise LoadError(f"Failed to get dataframe values. {e}")


    @patch("pg8000.dbapi.Connection")
    def test_get_dataframe_values_failure(mock_conn):
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        mock_conn.execute.side_effect = Exception("SQL error")

        with pytest.raises(LoadError) as excinfo:
            get_dataframe_values(mock_df, mock_conn, "test_table")
        
        assert "Failed to get dataframe values" in str(excinfo.value)
    
    @patch("pg8000.dbapi.Connection")
    def test_get_dataframe_values_failure(self, mock_connection):
        mock_connection.execute.side_effect = DatabaseError("Database query failed")
        
        df = pd.DataFrame({"col1": [1, 2, 3]})
        
        with pytest.raises(LoadError) as excinfo:
            get_dataframe_values(df, mock_connection, "mock_table")
        
        assert "Failed to get dataframe values" in str(excinfo.value)

    @patch("pg8000.dbapi.Connection")
    def test_store_table_in_wh_failure(self, mock_connection):
        mock_connection.cursor.return_value.executemany.side_effect = DatabaseError("Database insert failed")
        
        query = "INSERT INTO test_table (col1) VALUES (%s)"
        table_rows = [(1,), (2,)]
        
        with pytest.raises(LoadError) as excinfo:
            store_table_in_wh(mock_connection, query, table_rows, "test_table")
        
        assert "Failed to store test_table in warehouse db" in str(excinfo.value)
