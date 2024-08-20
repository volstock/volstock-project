import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import logging
from src.extract import get_bucket_name
import requests
import io
from src.extract import get_date

logging.basicConfig(level=50)


class ProcessError(Exception):
    pass


def lambda_handler(event, context):
    try:
        S3_INGEST_BUCKET = get_bucket_name("S3_INGEST_BUCKET")
        S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")
        table_names = event["tables"]
        print(table_names)
    except ProcessError as e:
        logging.critical(e)
        return {"msg": "Failed to process data", "err": e}


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


def get_dim_staff(df_staff, df_department):
    try:
        df_staff_department = df_staff.join(
            df_department.set_index("department_id"),
            how="inner",
            on="department_id",
            lsuffix="_staff",
            rsuffix="_dep",
        )
        return df_staff_department[
            [
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        ].set_index("staff_id")
    except Exception as e:
        raise ProcessError(f"Failed to get dim_staff. {e}")


def get_dim_location(df_address):
    df_location = df_address.rename(columns={"address_id": "location_id"})
    return df_location.drop(columns=["created_at", "last_updated"]).set_index(
        "location_id"
    )


def get_dim_design(df_design):
    try:
        return df_design.drop(columns=["created_at", "last_updated"]).set_index(
            "design_id"
        )
    except Exception as e:
        raise ProcessError(f"Failed to get dim_design. {e}")


def get_currency_names_dataframe():
    try:
        currencies = requests.get(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"
        ).json()
        return pd.DataFrame(
            {
                "currency_code": [key for key in currencies.keys()],
                "currency_name": [value for value in currencies.values()],
            }
        )
    except Exception as e:
        raise ProcessError(f"Failed to get currency_names_dataframe. {e}")


def get_dim_currency(df_currency):
    try:
        df_currency_names = get_currency_names_dataframe()
        df_currency["currency_code"] = df_currency["currency_code"].apply(
            lambda x: x.lower()
        )
        df_currency_codes_names = df_currency.join(
            df_currency_names.set_index("currency_code"),
            how="inner",
            on="currency_code",
            lsuffix="_c",
            rsuffix="_cn",
        )
        return df_currency_codes_names.drop(
            columns=["created_at", "last_updated"]
        ).set_index("currency_id")
    except Exception as e:
        raise ProcessError(f"Failed to get dim_currency. {e}")


def get_dim_counterparty(df_counterparty, df_address):
    try:
        df_counterparty_address = df_counterparty.join(
            df_address.set_index("address_id"),
            on="legal_address_id",
            lsuffix="_cp",
            rsuffix="_a",
        )[
            [
                "counterparty_id",
                "counterparty_legal_name",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ]
        ]
        return df_counterparty_address.rename(
            columns={
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            }
        ).set_index("counterparty_id")
    except Exception as e:
        raise ProcessError(f"Failed to get dim_counterparty. {e}")


def get_fact_sales_order(df_sales_order):
    try:
        df_sales_order["sales_record_id"] = range(1, len(df_sales_order) + 1)
        df_sales_order["created_date"] = df_sales_order["created_at"].apply(
            lambda x: x[: x.index(" ")]
        )
        df_sales_order["created_time"] = df_sales_order["created_at"].apply(
            lambda x: x[x.index(" ") + 1 :]
        )
        df_sales_order["last_updated_date"] = df_sales_order["last_updated"].apply(
            lambda x: x[: x.index(" ")]
        )
        df_sales_order["last_updated_time"] = df_sales_order["last_updated"].apply(
            lambda x: x[x.index(" ") + 1 :]
        )
        return (
            df_sales_order.rename(columns={"staff_id": "sales_staff_id"})
            .drop(columns=["created_at", "last_updated"])
            .set_index("sales_record_id")
        )
    except Exception as e:
        raise ProcessError(f"Failed to get fact_sales_order. {e}")


def get_dim_date(fact_sales_order):
    try:
        dates = pd.concat(
            [
                fact_sales_order["created_date"],
                fact_sales_order["last_updated_date"],
                fact_sales_order["agreed_payment_date"],
                fact_sales_order["agreed_delivery_date"],
            ]
        ).drop_duplicates(ignore_index=True)
        timestamps = dates.apply(lambda x: pd.to_datetime(x))
        dim_date = pd.DataFrame(
            {
                "date_id": timestamps,
            }
        )
        dim_date["year"] = dim_date["date_id"].dt.year
        dim_date["month"] = dim_date["date_id"].dt.month
        dim_date["day"] = dim_date["date_id"].dt.day
        dim_date["day_of_week"] = dim_date["date_id"].dt.day_of_week
        dim_date["day_name"] = dim_date["date_id"].dt.day_name()
        dim_date["month_name"] = dim_date["date_id"].dt.month_name()
        dim_date["quarter"] = dim_date["date_id"].dt.quarter
        return dim_date.set_index("date_id")
    except Exception as e:
        raise ProcessError(f"Failed to get dim_date. {e}")


def df_to_parquet(df):
    try:
        parquet_file = io.BytesIO()
        df.to_parquet(parquet_file, index=False)
        parquet_file.seek(0)
        return parquet_file
    except Exception as e:
        raise ProcessError(f"Failed to convert dataframe to parquet. {e}")


def store_parquet_file(bucket, parquet_file, table_name):
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.put_object(Body=parquet_file, Bucket=bucket, Key=f"{table_name}.parquet")
    except ClientError as e:
        raise ProcessError(f"Failed to store parquet_file in bucket. {e}")
