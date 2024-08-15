import unittest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
from src.extract import get_secrets, get_connection


class TestLambdaHandler(unittest.TestCase):
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
            self.assertIn(stored_secret, expected_secrets)

    @patch('boto3.client')
    @patch('pg8000.native.Connection')
    @patch('src.extract.get_secrets')
    def test_db_params_cll(self, mock_get_secrets, mock_pg_conn, mock_boto_ct):
        mock_sm_client = MagicMock()
        mock_boto_ct.return_value = mock_sm_client
        mock_get_secrets.return_value = {
            "database": "test_db",
            "host": "test_host",
            "user": "test_user",
            "password": "test_password"
         }
        mock_pg_conn.return_value = MagicMock()
        get_connection()

        mock_pg_conn.assert_called_once_with(
            database="test_db",
            host="test_host",
            user="test_user",
            password="test_password")
