from moto import mock_aws
import os
import boto3
import pytest
import unittest
from unittest.mock import patch, MagicMock
from src.load import get_connection, get_dim_payment_type_query, get_fact_purchase_order_query, get_fact_payment_query, get_dim_transaction_query, get_dim_transaction_query, get_fact_sales_order_query, get_secrets, get_dim_design_query, get_dim_staff_query, get_dim_location_query, get_dim_currency_query, get_dim_counterparty_query, get_dim_date_query


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

def test_get_dim_design_query():
    test_query = """
        INSERT INTO dim_design (
            design_id,
            design_name,
            file_location,
            file_name
        )
        VALUES (%s, %s, %s, %s)
    """
    test_func = get_dim_design_query()

    assert test_func == test_query

def test_get_dim_staff_query():
    test_query = """
        INSERT INTO dim_staff (
            staff_id,
            first_name,
            last_name,
            department_name,
            location,
            email_address
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    test_func = get_dim_staff_query()

    assert test_func == test_query

def test_get_dim_location_query():
    test_query = """
        INSERT INTO dim_location (
            location_id,
            address_line_1,
            address_line_2,
            district,
            city,
            postal_code,
            country,
            phone
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    test_func = get_dim_location_query()

    assert test_func == test_query

def test_get_dim_currency_query():
    test_query = """
        INSERT INTO dim_currency (
            currency_id,
            currency_code,
            currency_name
        )
        VALUES (%s, %s, %s)
    """
    test_func = get_dim_currency_query()

    assert test_func == test_query

def test_get_dim_counterparty_query():
    test_query = """
        INSERT INTO dim_counterparty (
            counterparty_id,
            counterparty_legal_name,
            counterparty_legal_address_line_1,
            counterparty_legal_address_line_2,
            counterparty_legal_district,
            counterparty_legal_city,
            counterparty_legal_postal_code,
            counterparty_legal_country,
            counterparty_legal_phone_number
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    test_func = get_dim_counterparty_query()

    assert test_func == test_query

def test_get_dim_date_query():
    test_query = """
        INSERT INTO dim_date (
            date_id,
            year,
            month,
            day,
            day_of_week,
            day_name,
            month_name,
            quarter
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    test_func = get_dim_date_query()

    assert test_func == test_query

def test_get_fact_sales_order_query():
    test_query = """
        INSERT INTO fact_sales_order (
            sales_order_id,
            created_date,
            created_time,
            last_updated_date,
            last_updated_time,
            sales_staff_id,
            counterparty_id,
            units_sold,
            unit_price,
            currency_id,
            design_id,
            agreed_payment_date,
            agreed_delivery_date,
            agreed_delivery_location_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    test_func = get_fact_sales_order_query()
    assert test_func == test_query

def test_get_dim_transaction_query():
    test_query = """
        INSERT INTO dim_transaction (
            transaction_id,
            transaction_type,
            sales_order_id,
            purchase_order_id
        )
        VALUES (%s, %s, %s, %s)
    """
    test_func = get_dim_transaction_query()
    assert test_func == test_query


def test_get_dim_payment_type_query():
    test_query = """
        INSERT INTO dim_payment_type (
            payment_type_id,
            payment_type_name
        )
        VALUES (%s, %s)
    """
    test_func = get_dim_payment_type_query()
    assert test_func == test_query

def test_get_fact_payment_query():
    test_query = """
        INSERT INTO fact_payment (
            payment_id,
            created_date,
            created_time,
            last_updated_date,
            last_updated_time,
            transaction_id,
            counterparty_id,
            payment_amount,
            currency_id,
            payment_type_id,
            paid,
            payment_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    test_func = get_fact_payment_query()
    assert test_func == test_query

def test_get_fact_purchase_order_query():
    test_query = """
        INSERT INTO fact_purchase_order (
            purchase_order_id,
            created_date,
            created_time,
            last_updated_date,
            last_updated_time,
            staff_id,
            counterparty_id,
            item_code,
            item_quantity,
            item_unit_price,
            currency_id,
            agreed_delivery_date,
            agreed_payment_date,
            agreed_delivery_location_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    test_func = get_fact_purchase_order_query()
    assert test_func == test_query
    
