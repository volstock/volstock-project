import boto3
import pandas as pd
from botocore.exceptions import ClientError
import pg8000.dbapi
from pg8000.exceptions import DatabaseError
import logging
import os
import io

logging.basicConfig(level=50)


class LoadError(Exception):
    pass


def lambda_handler(event, context):
    try:
        S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")
        conn = get_connection()
        tables_names = event["tables"]
        for table_name in tables_names:
            if table_name == "dim_design":
                dim_design = get_table_df_from_parquet(S3_PROCESS_BUCKET, table_name)
                query = get_dim_design_query()
                rows = get_dataframe_values(dim_design)
                store_table_in_wh(conn, query, rows, table_name)
            elif table_name == "dim_staff":
                dim_staff = get_table_df_from_parquet(S3_PROCESS_BUCKET, table_name)
                query = get_dim_staff_query()
                rows = get_dataframe_values(dim_staff)
                store_table_in_wh(conn, query, rows, table_name)
            elif table_name == "dim_location":
                dim_location = get_table_df_from_parquet(S3_PROCESS_BUCKET, table_name)
                query = get_dim_location_query()
                rows = get_dataframe_values(dim_location)
                store_table_in_wh(conn, query, rows, table_name)
            elif table_name == "dim_currency":
                dim_currency = get_table_df_from_parquet(S3_PROCESS_BUCKET, table_name)
                query = get_dim_currency_query()
                rows = get_dataframe_values(dim_currency)
                store_table_in_wh(conn, query, rows, table_name)
            elif table_name == "dim_counterparty":
                dim_counterparty = get_table_df_from_parquet(
                    S3_PROCESS_BUCKET, table_name
                )
                query = get_dim_counterparty_query()
                rows = get_dataframe_values(dim_counterparty)
                store_table_in_wh(conn, query, rows, table_name)
            elif table_name == "dim_date":
                dim_date = get_table_df_from_parquet(S3_PROCESS_BUCKET, table_name)
                query = get_dim_date_query()
                rows = get_dataframe_values(dim_date)
                store_table_in_wh(conn, query, rows, table_name)
            elif table_name == "fact_sales_order":
                fact_sales_order = get_table_df_from_parquet(
                    S3_PROCESS_BUCKET, table_name
                )
                query = get_fact_sales_order_query()
                rows = get_dataframe_values(fact_sales_order)
                store_table_in_wh(conn, query, rows, table_name)
        return {"msg": "Data process successful."}
    except LoadError as e:
        logging.critical(e)
        return {"msg": "Failed to load data into warehouse", "err": str(e)}


def get_bucket_name(bucket_name):
    try:
        bucket = os.environ[bucket_name]
        return bucket
    except KeyError as e:
        raise LoadError(f"Failed to get env bucket name. {e}")


def get_secrets(sm):
    """
    Retrieves Data Warehouse connection details from AWS Secrets Manager.

    Parameters:
    - sm (boto3.client): The Secrets Manager client.

    Returns:
    - A dictionary containing Data warehouse credentials including:
        - 'database','host','user','password'
    """
    try:
        db = sm.get_secret_value(SecretId="whdb_name")["SecretString"]
        host = sm.get_secret_value(SecretId="whdb_host")["SecretString"]
        user = sm.get_secret_value(SecretId="whdb_user")["SecretString"]
        password = sm.get_secret_value(SecretId="whdb_pass")["SecretString"]
        return {"database": db, "host": host, "user": user, "password": password}
    except ClientError as e:
        raise LoadError(f"Failed to get secrets. {e}")


def get_connection():
    """
    Establishes a connection to the Data Warehouse using credentials from
    AWS Secrets Manager.

    Returns:
    - pg8000.native.Connection: A connection object to the database.

    """
    try:
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        return pg8000.dbapi.connect(**get_secrets(sm))
    except DatabaseError as e:
        raise LoadError(f"Failed to get connection. {e}")


def get_table_df_from_parquet(bucket, parquet_name):
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        buffer = io.BytesIO()
        s3.download_fileobj(
            Bucket=bucket, Key=f"{parquet_name}.parquet", Fileobj=buffer
        )
        return pd.read_parquet(buffer).convert_dtypes().replace({pd.NA: None})
    except ClientError as e:
        raise LoadError(f"Failed to get dataframe from parquet file. {e}")


def get_dataframe_values(df):
    try:
        return df.values.tolist()
    except Exception as e:
        raise LoadError(f"Failed to get dataframe values. {e}")


def store_table_in_wh(conn, query, table_rows, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} *")
        conn.commit()
        cursor.executemany(query, table_rows)
        conn.commit()
    except DatabaseError as e:
        raise LoadError(f"Failed to store {table_name} in warehouse db. {e}")


def get_dim_design_query():
    query = """
        INSERT INTO dim_design (
            design_id,
            design_name,
            file_location,
            file_name
        )
        VALUES (%s, %s, %s, %s)
    """
    return query


def get_dim_staff_query():

    query = """
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
    return query


def get_dim_location_query():

    query = """
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
    return query


def get_dim_currency_query():
    query = """
        INSERT INTO dim_currency (
            currency_id,
            currency_code,
            currency_name
        )
        VALUES (%s, %s, %s)
    """
    return query


def get_dim_counterparty_query():
    query = """
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
    return query


def get_dim_date_query():
    query = """
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
    return query


def get_fact_sales_order_query():
    query = """
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
    return query


# def get_dim_transaction_query():
#     query = """
#         INSERT INTO dim_transaction (
#             transaction_id,
#             transaction_type,
#             sales_order_id,
#             purchase_order_id
#         )
#         VALUES (%s, %s, %s, %s)
#     """
#     return query


# def get_fact_payment_query():
#     query = """
#         INSERT INTO fact_payment (
#             payment_id,
#             created_date,
#             created_time,
#             last_updated_date,
#             last_updated_time,
#             transaction_id,
#             counterparty_id,
#             payment_amount,
#             currency_id,
#             payment_type_id,
#             paid,
#             payment_date
#         )
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """
#     return query

# def get_dim_payment_type_query():
#     query = """
#         INSERT INTO dim_payment_type (
#             payment_type_id,
#             payment_type_name
#         )
#         VALUES (%s, %s)
#     """
#     return query
