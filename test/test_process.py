import pytest
import os
import io
import json
import pandas as pd
from botocore.exceptions import ClientError
from unittest.mock import patch, MagicMock
from moto import mock_aws
import boto3
from src.process import (
    get_dataframe_from_table_json,
    ProcessError,
    get_dim_staff,
    get_dim_location,
    get_dim_design,
    get_dim_currency,
    get_currency_names_dataframe,
    get_dim_counterparty,
    get_dim_payment_type,
    get_dim_transaction,
    get_fact_payment,
    get_fact_purchase_order,
    get_fact_sales_order,
    get_bucket_name,
    get_date,
    get_dim_date,
    df_to_parquet,
    store_parquet_file,
    lambda_handler,
)

S3_MOCK_BUCKET_NAME = "mock-bucket-1"
MOCK_TABLE_NAME = "mock_table"
MOCK_JSON_TABLE = {"column1": ["data1", "data2"], "column2": ["data3", "data4"]}


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


@pytest.fixture(scope="function")
def s3_bucket(s3):
    s3.create_bucket(
        Bucket=S3_MOCK_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2",
        },
    )


@pytest.fixture(scope="function")
def s3_bucket_latest_date(s3, s3_bucket):
    s3.put_object(
        Body="2024-8-22".encode(), Bucket=S3_MOCK_BUCKET_NAME, Key="latest_date"
    )


@patch.dict(os.environ, {"S3_MOCK_BUCKET_NAME": S3_MOCK_BUCKET_NAME})
def test_get_bucket_name():
    assert get_bucket_name("S3_MOCK_BUCKET_NAME") == S3_MOCK_BUCKET_NAME


def test_get_bucket_name_error():
    with pytest.raises(ProcessError) as e:
        get_bucket_name("S3_MOCK_BUCKET_NAME")
    assert str(e.value) == "Failed to get env bucket name. 'S3_MOCK_BUCKET_NAME'"


def test_get_date(s3, s3_bucket_latest_date):
    assert get_date(S3_MOCK_BUCKET_NAME) == "2024-8-22"


def test_get_date_error(s3, s3_bucket):
    with pytest.raises(ProcessError) as e:
        get_date(S3_MOCK_BUCKET_NAME)
    assert (
        str(e.value) == "Failed to get date from bucket. An error occurred "
        "(NoSuchKey) when calling the GetObject operation: The specified key "
        "does not exist."
    )


def test_table_json_to_dataframe_success(s3, s3_bucket_latest_date):
    s3.put_object(
        Body=json.dumps(MOCK_JSON_TABLE).encode(),
        Bucket=S3_MOCK_BUCKET_NAME,
        Key=f"latest/2024-8-22/{MOCK_TABLE_NAME}.json",
    )
    df = get_dataframe_from_table_json(S3_MOCK_BUCKET_NAME, MOCK_TABLE_NAME)
    assert df.equals(pd.DataFrame(MOCK_JSON_TABLE))


def test_table_json_to_dataframe_error(s3_bucket_latest_date):
    with pytest.raises(ProcessError) as e:
        get_dataframe_from_table_json(S3_MOCK_BUCKET_NAME, MOCK_TABLE_NAME)
    assert (
        str(e.value) == "Failed to get table json. An error occurred "
        "(NoSuchKey) when calling the GetObject operation: The specified key "
        "does not exist."
    )


@pytest.fixture
def sample_staff():
    return pd.DataFrame(
        {
            "staff_id": [1, 2, 3],
            "first_name": ["John", "Jane", "Doe"],
            "last_name": ["Doe", "Smith", "Jones"],
            "department_id": [1, 2, 3],
            "location": ["New York", "London", "Paris"],
            "email_address": [
                "john@example.com",
                "jane@example.com",
                "doe@example.com",
            ],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"],
        }
    )


@pytest.fixture
def sample_department():
    return pd.DataFrame(
        {
            "department_id": [1, 2, 3],
            "department_name": ["HR", "Finance", "Engineering"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"],
        }
    )


@pytest.fixture
def sample_address():
    return pd.DataFrame(
        {
            "address_id": [100, 200, 300],
            "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
            "city": ["Metropolis", "Gotham", "Star City"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"],
        }
    )


def test_get_dim_staff(sample_staff, sample_department):
    result = get_dim_staff(sample_staff, sample_department)
    expected = pd.DataFrame(
        {
            "staff_id": [1, 2, 3],
            "first_name": ["John", "Jane", "Doe"],
            "last_name": ["Doe", "Smith", "Jones"],
            "department_name": ["HR", "Finance", "Engineering"],
            "location": ["New York", "London", "Paris"],
            "email_address": [
                "john@example.com",
                "jane@example.com",
                "doe@example.com",
            ],
        }
    )

    assert expected.equals(result)


@patch("pandas.DataFrame.join")
def test_get_dim_staff_error(mock_df_join, sample_staff, sample_department):
    mock_df_join.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_staff(sample_staff, sample_department)
    assert str(e.value) == "Failed to get dim_staff. Mock exception"


@pytest.fixture(scope="function")
def sample_address_alt():
    return pd.DataFrame(
        {
            "address_id": [1, 2, 3],
            "address_line_1": [
                "6826 Herzog Via",
                "179 Alexie Cliffs",
                "148 Sincere Fort",
            ],
            "city": ["Kendraburgh", "Suffolk", "Pricetown"],
            "created_at": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
            ],
            "last_updated": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
            ],
        }
    )


def test_get_dim_location(sample_address_alt):
    expected = pd.DataFrame(
        {
            "location_id": [1, 2, 3],
            "address_line_1": [
                "6826 Herzog Via",
                "179 Alexie Cliffs",
                "148 Sincere Fort",
            ],
            "city": ["Kendraburgh", "Suffolk", "Pricetown"],
        },
    )
    result = get_dim_location(sample_address_alt)
    assert expected.equals(result)


@patch("pandas.DataFrame.rename")
def test_get_dim_location_error(mock_df_rename, sample_address_alt):
    mock_df_rename.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_location(sample_address_alt)
    assert str(e.value) == "Failed to get dim_location. Mock exception"


@pytest.fixture(scope="function")
def sample_design():
    return pd.DataFrame(
        {
            "design_id": [1, 2, 3],
            "design_name": ["Wooden", "Bronze", "Soft"],
            "file_location": ["/usr", "/private", "/System"],
            "file_name": [
                "wooden-20220717-npgz.json",
                "bronze-20221024-4dds.json",
                "soft-20211001-cjaz.json",
            ],
            "created_at": [
                "2022-11-03 14:20:49.962000",
                "2023-01-12 18:50:09.935000",
                "2023-02-07 17:31:10.093000",
            ],
            "last_updated": [
                "2022-11-03 14:20:49.962000",
                "2022-11-22 15:02:10.226000",
                "2023-02-07 17:31:10.093000",
            ],
        }
    )


def test_get_dim_design(sample_design):
    result_df = get_dim_design(sample_design)
    expected_columns = ["design_id", "design_name", "file_location", "file_name"]

    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)
    assert len(result_df) == len(sample_design)


@patch("pandas.DataFrame.drop")
def testr_get_dim_design_error(mock_df_drop, sample_design):
    mock_df_drop.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_design(sample_design)
    assert str(e.value) == "Failed to get dim_design. Mock exception"


def test_get_currency_names_dataframe():

    mock_response_data = {
        "usd": "United States Dollar",
        "eur": "Euro",
        "gbp": "British Pound Sterling",
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_response_data

        result_df = get_currency_names_dataframe()

        expected_columns = ["currency_code", "currency_name"]

        assert isinstance(result_df, pd.DataFrame)
        assert all(col in result_df.columns for col in expected_columns)
        assert len(result_df) == len(mock_response_data)

        expected_data = [
            ("usd", "United States Dollar"),
            ("eur", "Euro"),
            ("gbp", "British Pound Sterling"),
        ]
        for code, name in expected_data:
            assert code in result_df["currency_code"].values
            assert name in result_df["currency_name"].values

        assert all(result_df["currency_code"].str.islower())

        mock_get.assert_called_once_with(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"
        )


@patch("requests.get")
def test_get_currency_names_dataframe_error(mock_req_get):
    mock_req_get.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_currency_names_dataframe()
    assert str(e.value) == "Failed to get currency_names_dataframe. Mock exception"


@pytest.fixture(scope="function")
def sample_currency():
    return pd.DataFrame(
        {
            "currency_id": [1, 2, 3],
            "currency_code": ["USD", "EUR", "GBP"],
            "created_at": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
            ],
            "last_updated": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
            ],
        }
    )


def test_get_dim_currency(sample_currency):
    mock_currency_names = pd.DataFrame(
        {
            "currency_code": ["usd", "eur", "gbp"],
            "currency_name": ["US Dollar", "Euro", "British Pound"],
        }
    )

    with patch("src.process.get_currency_names_dataframe") as mock_get_names:
        mock_get_names.return_value = mock_currency_names

        result_df = get_dim_currency(sample_currency)
        expected_columns = ["currency_id", "currency_code", "currency_name"]
        expected_names = ["US Dollar", "Euro", "British Pound"]

        assert isinstance(result_df, pd.DataFrame)
        assert "created_at" not in result_df.columns
        assert "last_updated" not in result_df.columns
        assert all(col in result_df.columns for col in expected_columns)
        assert result_df["currency_name"].tolist() == expected_names
        assert result_df["currency_code"].tolist() == ["usd", "eur", "gbp"]

        mock_get_names.assert_called_once()


@patch("src.process.get_currency_names_dataframe")
def test_get_dim_currency_error(mock_g_c_n_df, sample_currency):
    mock_g_c_n_df.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_currency(sample_currency)
    assert str(e.value) == "Failed to get dim_currency. Mock exception"


@pytest.fixture(scope="function")
def sample_counterparty():
    return pd.DataFrame(
        {
            "counterparty_id": [1, 2, 3],
            "counterparty_legal_name": [
                "Fahey and Sons",
                "Armstrong Inc",
                "Kohler Inc",
            ],
            "legal_address_id": [15, 28, 2],
            "commercial_contact": ["Micheal Toy", "Melba Sanford", "Homer Mitchell"],
            "delivery_contact": ["Myra Kovacek", "Eva Upton", "Veronica Fay"],
        }
    )


@pytest.fixture(scope="function")
def sample_address_alt_2():
    return pd.DataFrame(
        {
            "address_id": [15, 28, 2],
            "address_line_1": ["123 Main St", "456 Maple Ave", "789 Elm St"],
            "address_line_2": ["Suite 100", "Apt 202", "PO Box 303"],
            "district": ["North District", "East District", "West District"],
            "city": ["Springfield", "Shelbyville", "Ogdenville"],
            "postal_code": ["11111", "22222", "33333"],
            "country": ["USA", "USA", "USA"],
            "phone": ["555-1234", "555-5678", "555-9876"],
        }
    )


def test_get_dim_counterparty(sample_counterparty, sample_address_alt_2):
    result_df = get_dim_counterparty(sample_counterparty, sample_address_alt_2)

    expected_columns = [
        "counterparty_id" "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
    ]

    assert isinstance(result_df, pd.DataFrame)
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.index.name == "counterparty_id"
    assert result_df.loc[1, "counterparty_legal_name"] == "Fahey and Sons"
    assert result_df.loc[2, "counterparty_legal_address_line_1"] == "456 Maple Ave"
    assert result_df.loc[3, "counterparty_legal_phone_number"] == "555-9876"


@patch("pandas.DataFrame.join")
def test_get_dim_counterparty(mock_df_join, sample_counterparty, sample_address_alt_2):
    mock_df_join.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_counterparty(sample_counterparty, sample_address_alt_2)
    assert str(e.value) == "Failed to get dim_counterparty. Mock exception"


@pytest.fixture(scope="function")
def sample_payment_type():
    return pd.DataFrame(
        {
            "payment_type_id": [1, 2, 3, 4],
            "payment_type_name": [
                "SALES_RECEIPT",
                "SALES_REFUND",
                "PURCHASE_PAYMENT",
                "PURCHASE_REFUND",
            ],
            "created_at": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
            ],
            "last_updated": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
            ],
        }
    )


def test_get_dim_payment_type(sample_payment_type):

    result_df = get_dim_payment_type(sample_payment_type)
    expected_columns = ["payment_type_id", "payment_type_name"]

    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.loc[0, "payment_type_name"] == "SALES_RECEIPT"
    assert result_df.loc[1, "payment_type_name"] == "SALES_REFUND"
    assert result_df.loc[2, "payment_type_name"] == "PURCHASE_PAYMENT"
    assert result_df.loc[3, "payment_type_name"] == "PURCHASE_REFUND"


@patch("pandas.DataFrame.drop")
def test_get_dim_payment_type_error(mock_df_drop, sample_payment_type):
    mock_df_drop.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_payment_type(sample_payment_type)
    assert str(e.value) == "Failed to get dim_payment_type. Mock exception"


@pytest.fixture(scope="function")
def sample_transaction():
    return pd.DataFrame(
        {
            "transaction_id": [1, 2],
            "transaction_type": ["PURCHASE", "SALE"],
            "sales_order_id": [1, 2],
            "created_at": ["2023-01-01 09:00:00", "2023-01-02 10:15:00"],
            "last_updated": ["2023-01-01 09:30:00", "2023-01-02 10:45:00"],
        }
    )


def test_get_dim_transaction_type(sample_transaction):
    result_df = get_dim_transaction(sample_transaction)
    expected_columns = ["transaction_id", "transaction_type", "sales_order_id"]

    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)

    assert result_df.loc[0, "transaction_type"] == "PURCHASE"
    assert result_df.loc[0, "sales_order_id"] == 1
    assert result_df.loc[1, "transaction_type"] == "SALE"
    assert result_df.loc[1, "sales_order_id"] == 2


@patch("pandas.DataFrame.drop")
def test_get_dim_transaction_type_error(mock_df_drop, sample_transaction):
    mock_df_drop.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_transaction(sample_transaction)
    assert str(e.value) == "Failed to get dim_transaction. Mock exception"


@pytest.fixture(scope="function")
def sample_payment():
    return pd.DataFrame(
        {
            "payment_id": [2, 3, 5],
            "payment_amount": [
                "552548.62",
                "205952.22",
                "57067.20",
            ],  # Ensure amount is treated as float
            "transaction_id": [1, 2, 3],
            "counterparty_id": [2, 3, 4],
            "currency_id": [1, 2, 3],
            "payment_type_id": [1, 2, 3],
            "paid": [10, 12, 15],
            "payment_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "created_at": [
                "2022-11-03 14:20:52.187000",
                "2022-11-03 14:20:52.186000",
                "2022-11-03 14:20:52.187000",
            ],
            "last_updated": [
                "2022-11-03 14:20:52.187000",
                "2022-11-03 14:20:52.186000",
                "2022-11-03 14:20:52.187000",
            ],
            "company_ac_number": [67305075, 81718079, 66213052],  # Column to be dropped
            "counterparty_ac_number": [
                31622269,
                47839086,
                91659548,
            ],  # Column to be dropped
        }
    )


def test_get_fact_payment(sample_payment):
    result_df = get_fact_payment(sample_payment)
    expected_columns = [
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date",
    ]

    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert "company_ac_number" not in result_df.columns
    assert "counterparty_ac_number" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.index.name == "payment_record_id"
    assert list(result_df.index) == [1, 2, 3]

    assert result_df.loc[1, "payment_amount"] == "552548.62"
    assert result_df.loc[1, "created_date"] == "2022-11-03"
    assert result_df.loc[1, "created_time"] == "14:20:52.187000"
    assert result_df.loc[1, "last_updated_date"] == "2022-11-03"
    assert result_df.loc[1, "last_updated_time"] == "14:20:52.187000"


@patch("pandas.DataFrame.set_index")
def test_get_fact_payment_error(mock_df_s_i, sample_payment):
    mock_df_s_i.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_fact_payment(sample_payment)
    assert str(e.value) == "Failed to get fact_payment. Mock exception"


@pytest.fixture(scope="function")
def sample_purchase_order():
    return pd.DataFrame(
        {
            "purchase_order_id": [1, 2, 3],
            "item_quantity": [371, 286, 839],
            "created_at": [
                "2023-02-01 08:00:00",
                "2023-02-02 09:30:00",
                "2023-02-03 11:45:00",
            ],
            "last_updated": [
                "2023-02-01 08:30:00",
                "2023-02-02 10:00:00",
                "2023-02-03 12:15:00",
            ],
            "staff_id": [1, 2, 3],
            "counterparty_id": [1, 4, 2],
            "item_code": [1, 2, 3],
            "item_unit_price": [10, 15, 20],
            "currency_id": [2, 3, 4],
            "agreed_delivery_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "agreed_payment_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "agreed_delivery_location_id": [2, 3, 6],
        }
    )


def test_get_fact_purchase_order(sample_purchase_order):
    result_df = get_fact_purchase_order(sample_purchase_order)

    expected_columns = [
        "purchase_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "staff_id",
        "counterparty_id",
        "item_code",
        "item_quantity",
        "item_unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id",
    ]

    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.index.name == "purchase_record_id"
    assert list(result_df.index) == [1, 2, 3]

    assert result_df.loc[1, "purchase_order_id"] == 1
    assert result_df.loc[1, "item_quantity"] == 371
    assert result_df.loc[1, "created_date"] == "2023-02-01"
    assert result_df.loc[1, "created_time"] == "08:00:00"
    assert result_df.loc[1, "last_updated_date"] == "2023-02-01"
    assert result_df.loc[1, "last_updated_time"] == "08:30:00"


@patch("builtins.range")
def test_get_fact_purchase_order_error(mock_range, sample_purchase_order):
    mock_range.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_fact_purchase_order(sample_purchase_order)
    assert str(e.value) == "Failed to get fact_purchase_order. Mock exception"


@pytest.fixture(scope="function")
def sample_sales_order():
    return pd.DataFrame(
        {
            "sales_order_id": [1, 3, 4],
            "created_at": [
                "2023-02-01 08:00:00",
                "2023-02-02 09:30:00",
                "2023-02-03 11:45:00",
            ],
            "last_updated": [
                "2023-02-01 08:30:00",
                "2023-02-02 10:00:00",
                "2023-02-03 12:15:00",
            ],
            "staff_id": [1, 2, 1],
            "counterparty_id": [2, 3, 1],
            "units_sold": [10, 5, 20],
            "unit_price": [20, 20, 24],
            "currency_id": [2, 1, 1],
            "design_id": [1, 2, 3],
            "agreed_payment_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "agreed_delivery_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "agreed_delivery_location_id": [1, 2, 3],
        }
    )


def test_get_fact_sales_order(sample_sales_order):
    fact_sales_order = get_fact_sales_order(sample_sales_order)
    expected = pd.DataFrame(
        {
            "sales_record_id": [1, 2, 3],
            "sales_order_id": [1, 3, 4],
            "created_date": ["2023-02-01", "2023-02-02", "2023-02-03"],
            "created_time": ["08:00:00", "09:30:00", "11:45:00"],
            "last_updated_date": ["2023-02-01", "2023-02-02", "2023-02-03"],
            "last_updated_time": ["08:30:00", "10:00:00", "12:15:00"],
            "sales_staff_id": [1, 2, 1],
            "counterparty_id": [2, 3, 1],
            "units_sold": [10, 5, 20],
            "unit_price": [20, 20, 24],
            "currency_id": [2, 1, 1],
            "design_id": [1, 2, 3],
            "agreed_payment_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "agreed_delivery_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "agreed_delivery_location_id": [1, 2, 3],
        }
    ).set_index('sales_record_id')
    print(expected)
    print(fact_sales_order)
    assert expected.equals(fact_sales_order)


@patch("pandas.DataFrame.rename")
def test_get_fact_sales_order_error(mock_df_rename, sample_sales_order):
    mock_df_rename.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_fact_sales_order(sample_sales_order)
    assert str(e.value) == "Failed to get fact_sales_order. Mock exception"


@patch("pandas.date_range")
def test_get_dim_date(mock_date_range):
    mock_datetime_index = pd.DatetimeIndex(["2024-08-01", "2024-08-22"])
    mock_date_range.return_value = mock_datetime_index
    result = get_dim_date()
    expected = pd.DataFrame(
        {
            "date_id": mock_datetime_index,
        }
    )
    expected["year"] = expected["date_id"].dt.year
    expected["month"] = expected["date_id"].dt.month
    expected["day"] = expected["date_id"].dt.day
    expected["day_of_week"] = expected["date_id"].dt.day_of_week
    expected["day_name"] = expected["date_id"].dt.day_name()
    expected["month_name"] = expected["date_id"].dt.month_name()
    expected["quarter"] = expected["date_id"].dt.quarter
    assert expected.equals(result)


@patch("pandas.date_range")
def test_get_dim_date_error(mock_date_range):
    mock_date_range.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        get_dim_date()
    assert str(e.value) == "Failed to get dim_date. Mock exception"


def test_df_to_parquet():
    df = pd.DataFrame({"c1": [1, 2, 3]})
    df_parquet = df_to_parquet(df)
    df_parquet_read = pd.read_parquet(df_parquet)
    assert df_parquet_read.equals(df)


@patch("pandas.DataFrame.to_parquet")
def test_df_to_parquet_error(mock_to_parquet):
    mock_to_parquet.side_effect = Exception("Mock exception")
    with pytest.raises(ProcessError) as e:
        df_to_parquet(pd.DataFrame())
    assert str(e.value) == "Failed to convert dataframe to parquet. Mock exception"


def test_store_parquet_file(s3, s3_bucket):
    df = pd.DataFrame({"c1": [1, 2, 3]})
    parquet_file = df_to_parquet(df)
    store_parquet_file(S3_MOCK_BUCKET_NAME, parquet_file, "mock-parquet")
    buffer = io.BytesIO()
    s3.download_fileobj(S3_MOCK_BUCKET_NAME, "mock-parquet.parquet", buffer)
    df_parquet_read = pd.read_parquet(buffer)
    assert df_parquet_read.equals(df)


def test_store_parquet_file_error(s3):
    df = df = pd.DataFrame({"c1": [1, 2, 3]})
    parquet_file = df_to_parquet(df)
    with pytest.raises(ProcessError) as e:
        store_parquet_file(S3_MOCK_BUCKET_NAME, parquet_file, "mock-parquet")
    assert (
        str(e.value)
        == "Failed to store parquet_file in bucket. An error occurred (NoSuchBucket) "
        "when calling the PutObject operation: The specified bucket does not exist"
    )


@patch("src.process.store_parquet_file")
@patch("src.process.df_to_parquet")
@patch("src.process.get_dim_date")
@patch("src.process.get_fact_purchase_order")
@patch("src.process.get_fact_payment")
@patch("src.process.get_dim_payment_type")
@patch("src.process.get_dim_transaction")
@patch("src.process.get_fact_sales_order")
@patch("src.process.get_dim_counterparty")
@patch("src.process.get_dim_currency")
@patch("src.process.get_dim_design")
@patch("src.process.get_dim_location")
@patch("src.process.get_dim_staff")
@patch("src.process.get_dataframe_from_table_json")
@patch.dict(
    os.environ,
    {
        "S3_INGEST_BUCKET": "mock-ingest-bucket",
        "S3_PROCESS_BUCKET": "mock-process-bucket",
    },
)
def test_lambda_handler(
    mock_get_dataframe,
    mock_get_dim_staff,
    mock_get_dim_location,
    mock_get_dim_design,
    mock_get_dim_currency,
    mock_get_dim_counterparty,
    mock_get_fact_sales_order,
    mock_get_dim_transaction,
    mock_get_dim_payment_type,
    mock_get_fact_payment,
    mock_get_fact_purchase_order,
    mock_get_dim_date,
    mock_df_to_parquet,
    mock_store_parquet_file,
):
    event = {
        "tables": [
            "address",
            "staff",
            "payment",
            "department",
            "transaction",
            "currency",
            "payment_type",
            "sales_order",
            "counterparty",
            "purchase_order",
            "design",
        ]
    }
    assert lambda_handler(event, "") == {
        "msg": "Data process successful.",
        "tables": [
            "dim_location",
            "dim_staff",
            "fact_payment",
            "dim_transaction",
            "dim_currency",
            "dim_payment_type",
            "fact_sales_order",
            "dim_counterparty",
            "fact_purchase_order",
            "dim_design",
            "dim_date",
        ],
    }
