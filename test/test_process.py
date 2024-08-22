import pytest
import os
import json
import pandas as pd
from botocore.exceptions import ClientError
from unittest.mock import patch, MagicMock
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
    lambda_handler
)



S3_BUCKET = "ingest-bucket-20240820100859166800000001"
TABLE_NAME = "mock_table"
MOCK_JSON_DATA = {
    "column1": ["data1", "data2"],
    "column2": ["data3", "data4"]
}

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
    """Mock the S3 client."""
    with patch("boto3.client") as mock_s3_client:
        s3_client = mock_s3_client.return_value
        yield s3_client


def test_table_json_to_dataframe_success(s3):
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(MOCK_JSON_DATA).encode('utf-8'))
    }
    df = get_dataframe_from_table_json(S3_BUCKET, TABLE_NAME)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.equals(pd.DataFrame(MOCK_JSON_DATA))
    s3.get_object.assert_called_once_with(
        Bucket=S3_BUCKET,
        Key=f"latest/2024-08-20 10:14/{TABLE_NAME}.json"
    )


def test_table_json_to_dataframe_client_error(s3):
    s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
        "GetObject"
    )

    with pytest.raises(ProcessError) as exc_info:
        get_dataframe_from_table_json(S3_BUCKET, TABLE_NAME)

    assert "Failed to get table json." in str(exc_info.value)
    s3.get_object.assert_called_once_with(
        Bucket=S3_BUCKET,
        Key=f"latest/2024-08-20 10:14/{TABLE_NAME}.json"
    )


@patch("src.process.get_bucket_name")
def test_lambda_handler_env_vars(mock_get_bucket_name):
    mock_get_bucket_name.side_effect = lambda name: f"mock-{name.lower()}"
    lambda_handler({}, {})
    mock_get_bucket_name.assert_any_call("S3_INGEST_BUCKET")
    mock_get_bucket_name.assert_any_call("S3_PROCESS_BUCKET")

@pytest.fixture
def sample_staff():
    return pd.DataFrame({
        "staff_id": [1, 2, 3],
        "first_name": ["John", "Jane", "Doe"],
        "last_name": ["Doe", "Smith", "Jones"],
        "department_id": [10, 20, 30],
        "location": ["New York", "London", "Paris"],  
        "email_address": ["john@example.com", "jane@example.com", "doe@example.com"], 
        "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"]
    })

@pytest.fixture
def sample_department():
    return pd.DataFrame({
        "department_id": [10, 20, 30],
        "department_name": ["HR", "Finance", "Engineering"],
        "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"]
    })

@pytest.fixture
def sample_address():
    return pd.DataFrame({
        "address_id": [100, 200, 300],
        "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
        "city": ["Metropolis", "Gotham", "Star City"],
        "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "last_updated": ["2024-01-10", "2024-01-11", "2024-01-12"]
    })


def test_get_dim_staff(sample_staff, sample_department):
    result = get_dim_staff(sample_staff, sample_department)
    expected = pd.DataFrame({
        "staff_id": [1, 2, 3],
        "first_name": ["John", "Jane", "Doe"],
        "last_name": ["Doe", "Smith", "Jones"],
        "department_name": ["HR", "Finance", "Engineering"],
        "location": ["New York", "London", "Paris"],
        "email_address": ["john@example.com", "jane@example.com", "doe@example.com"]
    })

    pd.testing.assert_frame_equal(result, expected)

def test_get_dim_location(sample_address):
    result = get_dim_location(sample_address)
    expected = pd.DataFrame({
        "location_id": [100, 200, 300],
        "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
        "city": ["Metropolis", "Gotham", "Star City"]
    })
    pd.testing.assert_frame_equal(result, expected)


def test_get_dim_design():
    sample_data = {
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
    df_design = pd.DataFrame(sample_data)
    result_df = get_dim_design(df_design)
    expected_columns = ["design_name", "file_location", "file_name"]


    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.index.name == "design_id"
    assert len(result_df) == len(df_design)



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


def test_get_dim_currency():
    sample_currency_data = {
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
    df_currency = pd.DataFrame(sample_currency_data)

    mock_currency_names = pd.DataFrame(
        {
            "currency_code": ["usd", "eur", "gbp"],
            "currency_name": ["US Dollar", "Euro", "British Pound"],
        }
    )

    with patch("src.process.get_currency_names_dataframe") as mock_get_names:
        mock_get_names.return_value = mock_currency_names

        result_df = get_dim_currency(df_currency)
        expected_columns = ["currency_code", "currency_name"]
        expected_names = ["US Dollar", "Euro", "British Pound"]

        assert isinstance(result_df, pd.DataFrame)
        assert "created_at" not in result_df.columns
        assert "last_updated" not in result_df.columns
        assert all(col in result_df.columns for col in expected_columns)
        assert result_df.index.name == "currency_id"
        assert result_df["currency_name"].tolist() == expected_names
        assert result_df["currency_code"].tolist() == ["usd", "eur", "gbp"]
        assert list(result_df.index) == [1, 2, 3]

        mock_get_names.assert_called_once()

def test_get_dim_counterparty():
    df_counterparty = pd.DataFrame(
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

    df_address = pd.DataFrame(
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

    result_df = get_dim_counterparty(df_counterparty, df_address)

    expected_columns = [
        "counterparty_legal_name",
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
    assert list(result_df.index) == [1, 2, 3]


def test_get_dim_payment_type():

    df_payment_type = pd.DataFrame(
        {
            "payment_type_id": [1, 2, 3, 4],
            "payment_type_name": [
                "SALES_RECEIPT",
                "SALES_REFUND",
                "PURCHASE_PAYMENT",
                "PURCHASE_REFUND"
            ],
            "created_at": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000"
            ],
            "last_updated": [
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000",
                "2022-11-03 14:20:49.962000"
            ]
        }
    )
    

    result_df = get_dim_payment_type(df_payment_type)
    expected_columns = ["payment_type_name"]

    assert isinstance(result_df, pd.DataFrame)  
    assert "created_at" not in result_df.columns  
    assert "last_updated" not in result_df.columns  
    assert all(col in result_df.columns for col in expected_columns) 
    assert result_df.index.name == "payment_type_id"  
    assert list(result_df.index) == [1, 2, 3, 4] 
    assert result_df.loc[1, "payment_type_name"] == "SALES_RECEIPT" 
    assert result_df.loc[2, "payment_type_name"] == "SALES_REFUND"  
    assert result_df.loc[3, "payment_type_name"] == "PURCHASE_PAYMENT"  
    assert result_df.loc[4, "payment_type_name"] == "PURCHASE_REFUND"


def test_get_dim_transaction_type():

    df_transaction_type = pd.DataFrame(
        {
            "transaction_id": [1, 2],
            "transaction_type": ["PURCHASE", "SALE"],
            "sales_order_id": [1, 2],
            "created_at": [
                "2023-01-01 09:00:00",
                "2023-01-02 10:15:00"
            ],
            "last_updated": [
                "2023-01-01 09:30:00",
                "2023-01-02 10:45:00"
            ]
        }
    )
    
    result_df = get_dim_transaction(df_transaction_type)
    expected_columns = ["transaction_type", "sales_order_id"]


    assert isinstance(result_df, pd.DataFrame) 
    assert "created_at" not in result_df.columns  
    assert "last_updated" not in result_df.columns  
    assert all(col in result_df.columns for col in expected_columns) 
    assert result_df.index.name == "transaction_id"  
    assert list(result_df.index) == [1, 2]  

    assert result_df.loc[1, "transaction_type"] == "PURCHASE"
    assert result_df.loc[1, "sales_order_id"] == 1
    assert result_df.loc[2, "transaction_type"] == "SALE"
    assert result_df.loc[2, "sales_order_id"] == 2


def test_get_fact_payment():

    df_payment = pd.DataFrame(
        {
            "payment_id": [2, 3, 5],
            "amount": ["552548.62", "205952.22", "57067.20"],  # Ensure amount is treated as float
            "created_at": [
                "2022-11-03 14:20:52.187000",
                "2022-11-03 14:20:52.186000",
                "2022-11-03 14:20:52.187000"
            ],
            "last_updated": [
                "2022-11-03 14:20:52.187000",
                "2022-11-03 14:20:52.186000",
                "2022-11-03 14:20:52.187000"
            ],
            "company_ac_number": [67305075, 81718079, 66213052],  # Column to be dropped
            "counterparty_ac_number": [31622269, 47839086, 91659548]  # Column to be dropped
        }
    )


    result_df = get_fact_payment(df_payment)

    expected_columns = [
        "amount",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time"
    ]

    assert isinstance(result_df, pd.DataFrame) 
    assert "created_at" not in result_df.columns 
    assert "last_updated" not in result_df.columns 
    assert "company_ac_number" not in result_df.columns 
    assert "counterparty_ac_number" not in result_df.columns  
    assert all(col in result_df.columns for col in expected_columns) 
    assert result_df.index.name == "payment_record_id" 
    assert list(result_df.index) == [1, 2, 3]  

    assert result_df.loc[1, "amount"] == '552548.62'
    assert result_df.loc[1, "created_date"] == "2022-11-03"
    assert result_df.loc[1, "created_time"] == "14:20:52.187000"
    assert result_df.loc[1, "last_updated_date"] == "2022-11-03"
    assert result_df.loc[1, "last_updated_time"] == "14:20:52.187000"


def test_get_fact_purchase_order():
    df_purchase_order = pd.DataFrame(
        {
            "purchase_order_id": [1, 2, 3],
            "item_quantity": [371, 286, 839],
            "created_at": [
                "2023-02-01 08:00:00",
                "2023-02-02 09:30:00",
                "2023-02-03 11:45:00"
            ],
            "last_updated": [
                "2023-02-01 08:30:00",
                "2023-02-02 10:00:00",
                "2023-02-03 12:15:00"
            ]
        }
    )
    
    result_df = get_fact_purchase_order(df_purchase_order)

    expected_columns = [
        "purchase_order_id",
        "item_quantity",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time"
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

def test_get_fact_sales_order_returns_correct_output():
    mock_df = pd.DataFrame({
        'staff_id': [1, 2],
        'created_at': ['2024-08-20 10:15:20', '2024-08-20 11:22:33'],
        'last_updated': ['2024-08-21 12:24:36', '2024-08-20 11:22:33']
    })

    expected_df = pd.DataFrame({
        'sales_staff_id': [101, 102],
        'created_date': ['2023-08-15', '2023-08-16'],
        'created_time': ['10:15:30', '11:20:45'],
        'last_updated_date': ['2023-08-17', '2023-08-18'],
        'last_updated_time': ['12:30:00', '13:35:15']
    }, index=pd.Index([1, 2], name='sales_record_id'))

    result_df = get_fact_sales_order(mock_df)

    try:
        pd.testing.assert_frame_equal(result_df, expected_df)
        print("Test passed! The result matches the expected DataFrame.")
    except AssertionError as e:
        print("Test failed! The result does not match the expected DataFrame.")
        print(e)