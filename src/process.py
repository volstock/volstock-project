import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import logging
import requests
import io
import os

logging.basicConfig(level=50)


class ProcessError(Exception):
    pass


def lambda_handler(event, context):
    try:
        S3_INGEST_BUCKET = get_bucket_name("S3_INGEST_BUCKET")
        S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")
        tables_names = event["tables"]
        update_tables_names = []
        fact_purchase_order = None
        fact_payment = None
        fact_sales_order = None
        for table_name in tables_names:
            if table_name == "staff":
                df_staff = get_dataframe_from_table_json(S3_INGEST_BUCKET, table_name)
                df_department = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, "department"
                )
                dim_staff = get_dim_staff(df_staff, df_department)
                dim_staff_parquet = df_to_parquet(dim_staff)
                store_parquet_file(S3_PROCESS_BUCKET, dim_staff_parquet, "dim_staff")
                insert_table_to_update_tables_arr(update_tables_names, "dim_staff")
            elif table_name == "address":
                df_address = get_dataframe_from_table_json(S3_INGEST_BUCKET, table_name)
                dim_location = get_dim_location(df_address)
                dim_location_parquet = df_to_parquet(dim_location)
                store_parquet_file(
                    S3_PROCESS_BUCKET, dim_location_parquet, "dim_location"
                )
                insert_table_to_update_tables_arr(update_tables_names, "dim_location")
            elif table_name == "design":
                df_design = get_dataframe_from_table_json(S3_INGEST_BUCKET, table_name)
                dim_design = get_dim_design(df_design)
                dim_design_parquet = df_to_parquet(dim_design)
                store_parquet_file(S3_PROCESS_BUCKET, dim_design_parquet, "dim_design")
                insert_table_to_update_tables_arr(update_tables_names, "dim_design")
            elif table_name == "currency":
                df_currency = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, table_name
                )
                dim_currency = get_dim_currency(df_currency)
                dim_currency_parquet = df_to_parquet(dim_currency)
                store_parquet_file(
                    S3_PROCESS_BUCKET, dim_currency_parquet, "dim_currency"
                )
                insert_table_to_update_tables_arr(update_tables_names, "dim_currency")
            elif table_name == "counterparty":
                df_counterparty = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, table_name
                )
                df_address = get_dataframe_from_table_json(S3_INGEST_BUCKET, "address")
                dim_counterparty = get_dim_counterparty(df_counterparty, df_address)
                dim_counterparty_parquet = df_to_parquet(dim_counterparty)
                store_parquet_file(
                    S3_PROCESS_BUCKET, dim_counterparty_parquet, "dim_counterparty"
                )
                insert_table_to_update_tables_arr(
                    update_tables_names, "dim_counterparty"
                )
            elif table_name == "sales_order":
                df_sales_order = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, table_name
                )
                fact_sales_order = get_fact_sales_order(df_sales_order)
                fact_sales_order_parquet = df_to_parquet(fact_sales_order)
                store_parquet_file(
                    S3_PROCESS_BUCKET, fact_sales_order_parquet, "fact_sales_order"
                )
                insert_table_to_update_tables_arr(
                    update_tables_names, "fact_sales_order"
                )
            elif table_name == "transaction":
                df_transaction = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, table_name
                )
                dim_transaction = get_dim_transaction(df_transaction)
                dim_transaction_parquet = df_to_parquet(dim_transaction)
                store_parquet_file(
                    S3_PROCESS_BUCKET, dim_transaction_parquet, "dim_transaction"
                )
                insert_table_to_update_tables_arr(
                    update_tables_names, "dim_transaction"
                )
            elif table_name == "payment_type":
                df_payment_type = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, table_name
                )
                dim_payment_type = get_dim_payment_type(df_payment_type)
                dim_payment_type_parquet = df_to_parquet(dim_payment_type)
                store_parquet_file(
                    S3_PROCESS_BUCKET, dim_payment_type_parquet, "dim_payment_type"
                )
                insert_table_to_update_tables_arr(
                    update_tables_names, "dim_payment_type"
                )
            elif table_name == "payment":
                df_payment = get_dataframe_from_table_json(S3_INGEST_BUCKET, table_name)
                fact_payment = get_fact_payment(df_payment)
                fact_payment_parquet = df_to_parquet(fact_payment)
                store_parquet_file(
                    S3_PROCESS_BUCKET, fact_payment_parquet, "fact_payment"
                )
                insert_table_to_update_tables_arr(update_tables_names, "fact_payment")
            elif table_name == "purchase_order":
                df_purchase_order = get_dataframe_from_table_json(
                    S3_INGEST_BUCKET, table_name
                )
                fact_purchase_order = get_fact_purchase_order(df_purchase_order)
                fact_purchase_order_parquet = df_to_parquet(fact_purchase_order)
                store_parquet_file(
                    S3_PROCESS_BUCKET,
                    fact_purchase_order_parquet,
                    "fact_purchase_order",
                )
                insert_table_to_update_tables_arr(
                    update_tables_names, "fact_purchase_order"
                )
        if (
            fact_sales_order is not None
            or fact_payment is not None
            or fact_purchase_order is not None
        ):
            dim_date = get_dim_date(
                S3_PROCESS_BUCKET, fact_sales_order, fact_payment, fact_purchase_order
            )
            dim_date_parquet = df_to_parquet(dim_date)
            store_parquet_file(S3_PROCESS_BUCKET, dim_date_parquet, "dim_date")
            insert_table_to_update_tables_arr(update_tables_names, "dim_date")
        return {"msg": "Data process successful.", "tables": update_tables_names}
    except ProcessError as e:
        logging.critical(e)
        return {"msg": "Failed to process data", "err": str(e)}


def insert_table_to_update_tables_arr(update_tables_names, table_name):
    i = len(update_tables_names) - 1
    update_tables_names.append(table_name)
    while i >= 0 and table_name < update_tables_names[i]:
        update_tables_names[i + 1] = update_tables_names[i]
        i -= 1
    update_tables_names[i + 1] = table_name


def get_bucket_name(bucket_name):
    try:
        bucket = os.environ[bucket_name]
        return bucket
    except KeyError as e:
        raise ProcessError(f"Failed to get env bucket name. {e}")


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
        ]
    except Exception as e:
        raise ProcessError(f"Failed to get dim_staff. {e}")


def get_dim_location(df_address):
    try:
        df_location = df_address.rename(columns={"address_id": "location_id"})
        return df_location.drop(columns=["created_at", "last_updated"])
    except Exception as e:
        raise ProcessError(f"Failed to get dim_location. {e}")


def get_dim_design(df_design):
    try:
        return df_design.drop(columns=["created_at", "last_updated"])
    except Exception as e:
        raise ProcessError(f"Failed to get dim_design. {e}")


def get_currency_names_dataframe():
    try:
        currencies = requests.get(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/"
            "currencies.json"
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
        return df_currency_codes_names.drop(columns=["created_at", "last_updated"])
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
        )
    except Exception as e:
        raise ProcessError(f"Failed to get dim_counterparty. {e}")


def get_dim_payment_type(df_payment_type):
    try:
        return df_payment_type.drop(columns=["created_at", "last_updated"])
    except Exception as e:
        raise ProcessError(f"Failed to get dim_payment_type. {e}")


def get_dim_transaction(df_transaction):
    try:
        return df_transaction.drop(columns=["created_at", "last_updated"])
    except Exception as e:
        raise ProcessError(f"Failed to get dim_transaction. {e}")


def get_fact_payment(df_payment):
    try:
        df_payment["payment_record_id"] = range(1, len(df_payment) + 1)
        df_payment["created_date"] = df_payment["created_at"].apply(
            lambda x: x[: x.index(" ")]
        )
        df_payment["created_time"] = df_payment["created_at"].apply(
            lambda x: x[x.index(" ") + 1 :]
        )
        df_payment["last_updated_date"] = df_payment["last_updated"].apply(
            lambda x: x[: x.index(" ")]
        )
        df_payment["last_updated_time"] = df_payment["last_updated"].apply(
            lambda x: x[x.index(" ") + 1 :]
        )
        return df_payment.drop(
            columns=[
                "created_at",
                "last_updated",
                "company_ac_number",
                "counterparty_ac_number",
            ]
        ).set_index("payment_record_id")[
            [
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
        ]
    except Exception as e:
        raise ProcessError(f"Failed to get fact_payment. {e}")


def get_fact_purchase_order(df_purchase_order):
    try:
        df_purchase_order["purchase_record_id"] = range(1, len(df_purchase_order) + 1)
        df_purchase_order["created_date"] = df_purchase_order["created_at"].apply(
            lambda x: x[: x.index(" ")]
        )
        df_purchase_order["created_time"] = df_purchase_order["created_at"].apply(
            lambda x: x[x.index(" ") + 1 :]
        )
        df_purchase_order["last_updated_date"] = df_purchase_order[
            "last_updated"
        ].apply(lambda x: x[: x.index(" ")])
        df_purchase_order["last_updated_time"] = df_purchase_order[
            "last_updated"
        ].apply(lambda x: x[x.index(" ") + 1 :])
        return df_purchase_order.drop(columns=["created_at", "last_updated"]).set_index(
            "purchase_record_id"
        )[
            [
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
        ]
    except Exception as e:
        raise ProcessError(f"Failed to get fact_purchase_order. {e}")


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
            .set_index("sales_record_id")[
                [
                    "sales_order_id",
                    "created_date",
                    "created_time",
                    "last_updated_date",
                    "last_updated_time",
                    "sales_staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "design_id",
                    "agreed_payment_date",
                    "agreed_delivery_date",
                    "agreed_delivery_location_id",
                ]
            ]
        )
    except Exception as e:
        raise ProcessError(f"Failed to get fact_sales_order. {e}")


def get_dim_date(bucket, fact_sales_order, fact_payment, fact_purchase_order):
    try:
        dates = []
        s3 = boto3.client("s3", region_name="eu-west-2")
        try:
            buffer = io.BytesIO()
            s3.download_fileobj(Bucket=bucket, Key=f"dim_date.parquet", Fileobj=buffer)
            s3_dates = pd.read_parquet(buffer)["date_id"].apply(lambda x: x.strftime('%Y-%m-%d'))
            dates.append(s3_dates)
        except ClientError:
            pass
        if fact_sales_order is not None:
            dates += [
                fact_sales_order["created_date"],
                fact_sales_order["last_updated_date"],
                fact_sales_order["agreed_payment_date"],
                fact_sales_order["agreed_delivery_date"],
            ]
        if fact_payment is not None:
            dates += [
                fact_payment["created_date"],
                fact_payment["last_updated_date"],
                fact_payment["payment_date"],
            ]
        if fact_purchase_order is not None:
            dates += [
                fact_purchase_order["created_date"],
                fact_purchase_order["last_updated_date"],
                fact_purchase_order["agreed_payment_date"],
                fact_purchase_order["agreed_delivery_date"],
            ]
        timestamps = (
            pd.concat(dates)
            .drop_duplicates(ignore_index=True)
            .apply(lambda x: pd.to_datetime(x))
        )
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
        return dim_date.sort_values(by='date_id', ignore_index=True)
    except Exception as e:
        raise ProcessError(f"Failed to get dim_date. {e}")


def df_to_parquet(df):
    try:
        parquet_file = io.BytesIO()
        parquet_file_close = parquet_file.close
        parquet_file.close = lambda: None
        df.to_parquet(parquet_file)
        parquet_file.close = parquet_file_close
        parquet_file.seek(0)
        return parquet_file
    except Exception as e:
        raise ProcessError(f"Failed to convert dataframe to parquet. {e}")


def store_parquet_file(bucket, parquet_file, parquet_name):
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.put_object(Body=parquet_file, Bucket=bucket, Key=f"{parquet_name}.parquet")
    except ClientError as e:
        raise ProcessError(f"Failed to store parquet_file in bucket. {e}")
