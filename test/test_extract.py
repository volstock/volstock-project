import unittest
from unittest.mock import patch
from moto import mock_secretsmanager
import boto3
from src.extract import lambda_handler


class TestLambdaHandler(unittest.TestCase):

    @mock_secretsmanager
    @patch('pg8000.native.Connection')
    def test_secrets_manager_retrieval(self, mock_pg_connection):
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        sm.create_secret(Name="db_name", SecretString="test_db")
        sm.create_secret(Name="db_host", SecretString="test_host")
        sm.create_secret(Name="db_user", SecretString="test_user")
        sm.create_secret(Name="db_pass", SecretString="test_pass")

        # Mock the pg8000 connection to prevent actual DB connection
        # mock_conn_instance = mock_pg_connection.return_value

        # Call the lambda_handler function
        event = {}
        context = {}
        lambda_handler(event, context)

        # Assert that Secrets Manager was queried for the correct secrets
        secret_calls = [
            unittest.mock.call(SecretId="db_name"),
            unittest.mock.call(SecretId="db_host"),
            unittest.mock.call(SecretId="db_user"),
            unittest.mock.call(SecretId="db_pass"),
        ]
        self.assertEqual(sm.get_secret_value.call_count, 4)
        sm.get_secret_value.assert_has_calls(secret_calls, any_order=True)

        # Assert that the database connection was created with the correct parameters
        # mock_pg_connection.assert_called_once_with(
        #     database="test_db",
        #     host="test_host",
        #     user="test_user",
        #     password="test_pass"
        # )

if __name__ == "__main__":
    unittest.main()
