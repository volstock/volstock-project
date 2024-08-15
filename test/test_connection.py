import unittest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
from src.extract import get_secrets, get_connection
import pytest
import os



class TestLambdaHandler(unittest.TestCase):

    @pytest.fixture(scope="function")
    def aws_credentials(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


    @pytest.fixture(scope="function")
    def sm(self, aws_credentials):
        with mock_aws():
            yield boto3.client("secretsmanager", region_name="eu-west-2")

    @mock_aws
    def test_ability_to_get_secrets_from_secret_manger_when_credentials_for_secret_manager_are_present(self): #testing 1-5
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
    def test_database_called_with_correct_keyword_arguments(self, mock_get_secrets, mock_pg_connection, mock_boto_client):
        mock_sm_client = MagicMock()
        mock_boto_client.return_value = mock_sm_client
        mock_get_secrets.return_value = {
            "database": "test_db",
            "host": "test_host",
            "user": "test_user",
            "password": "test_password"
         }
        mock_pg_connection.return_value = MagicMock()
        connection = get_connection()

        mock_pg_connection.assert_called_once_with(
            database="test_db",
            host="test_host",
            user="test_user",
            password="test_password")
