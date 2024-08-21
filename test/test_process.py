import pandas as pd
import pytest
import json
from unittest.mock import patch
import boto3
from botocore.exceptions import ClientError

class ProcessError(Exception):
    pass

def get_date(bucket):
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        date_object = s3.get_object(Bucket=bucket, Key="latest_date")
        return date_object["Body"].read().decode()
    except ClientError as e:
        raise ProcessError(f"Failed to get date from bucket. {e}")

def get_dataframe_from_table_json(bucket, table_name):
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        latest_date = get_date(bucket)
        table_json = (
            s3.get_object(
                Bucket=bucket,
                Key=f"latest/{latest_date}/{table_name}.json",
            )["Body"]
            .read()
            .decode()
        )
        return pd.DataFrame(json.loads(table_json))
    except ClientError as e:
        raise ProcessError(f"Failed to get table json. {e}")

def get_dim_staff(staff, department):
    df = staff.merge(department, on="department_id", how="left")
    df = df.rename(columns={"department_name": "department"})
    df = df[["staff_id", "first_name", "last_name", "department", "location", "email_address"]]
    return df

def get_dim_location(address):
    df = address[["location_id", "address_line_1", "city"]]
    return df.set_index("location_id")

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
        "location_id": [100, 200, 300],
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
        "department": ["HR", "Finance", "Engineering"],
        "location": ["New York", "London", "Paris"],
        "email_address": ["john@example.com", "jane@example.com", "doe@example.com"]
    })
    pd.testing.assert_frame_equal(result, expected)

def test_get_dim_location(sample_address):
    result = get_dim_location(sample_address)
    expected = pd.DataFrame({
        "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
        "city": ["Metropolis", "Gotham", "Star City"]
    }, index=pd.Index([100, 200, 300], name="location_id"))
    pd.testing.assert_frame_equal(result, expected)

