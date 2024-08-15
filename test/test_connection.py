import unittest
from unittest.mock import patch
import boto3
from moto import mock_aws
from src.extract import get_connection, IngestError, ClientError
import pytest

class TestLambdaHandler(unittest.TestCase):

    @mock_aws
    @patch('pg8000.native.Connection')
    def test_secrets_manager_retrieval(self, mock_pg_connection):
        """
        Set up mock Secrets Manager
        """
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        #empty sm
        
        sm.create_secret(Name="db_name", SecretString="test_db")
        sm.create_secret(Name="db_host", SecretString="test_host")
        sm.create_secret(Name="db_user", SecretString="test_user")
        sm.create_secret(Name="db_pass", SecretString="test_pass")
        #sm contains 4 secrets
        
        """
        Call the lambda_handler function
        """
        get_connection()
        
        """
        Assert that Secrets Manager was queried for the correct secrets
        """

        secrets = sm.list_secrets()['SecretList']
        secret_names = [secret['Name'] for secret in secrets]
        expected_secrets = ["db_name", "db_host", "db_user", "db_pass"]
        for secret in expected_secrets:
            self.assertIn(secret, secret_names)
        
        """
        Assert that the database connection was created with the correct parameters
        """

        mock_pg_connection.assert_called_once_with(
            database="test_db",
            host="test_host",
            user="test_user",
            password="test_pass"
        )
        
    # def test_incorrect_secrets_credentials_returns_Client_Error(self):
    #     #create a mock version of get_connection
    #     with patch('src.extract.get_connection') as mock_get_connection:
            
        
    #     with pytest.raises(IngestError):
    #         get_connection()



