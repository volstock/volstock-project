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
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        conn = get_connection(sm)
        changed_tables = event["tables"]
        table_queries = get_table_queries()

        for table in changed_tables:
            if table in table_queries:
                df = get_df_from_parquet(f"{table}.parquet")
                query = table_queries[table]
                rows = get_dataframe_values(df)
                store_table_in_wh(conn, query, rows, table)
            else: print("Did not have query prepared for that table. Please contact devs and ask them to make one")
        return {"msg": "Data process successful."}
    except LoadError as e:
        logging.critical(e)
        return {"msg": "Failed to load data into warehouse", "err": str(e)}

def get_table_queries():
    return { #A table containing the queries needed to insert data into the table
            "dim_design": get_dim_design_query,
            "dim_staff": get_dim_staff_query,
            "dim_location": get_dim_location_query,
            "dim_currency": get_dim_currency_query,
            "dim_counterparty": get_dim_counterparty_query,
            "dim_date": get_dim_date_query,
            "fact_sales_order": get_fact_sales_order_query
        }


def get_bucket_name(bucket_name):
    try:
        return os.environ[bucket_name]
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


def get_connection(sm):
    """
    Establishes a connection to the Data Warehouse using credentials from
    AWS Secrets Manager.

    Returns:
    - pg8000.native.Connection: A connection object to the database.

    """
    try:
        return pg8000.dbapi.connect(**get_secrets(sm))
    except DatabaseError as e:
        raise LoadError(f"Failed to get connection. {e}")

def get_df_from_parquet(parquet_file_key):
    s3 = boto3.client('s3')
    bucket = get_bucket_name("S3_PROCESS_BUCKET")
    parquet_file_object = s3.get_object(
        Bucket = bucket,
        Key = parquet_file_key
    )

    parquet_file = io.BytesIO(parquet_file_object["Body"].read())
    df = pd.read_parquet(parquet_file)
    return df 



def get_dataframe_values(df):
    try:
        return df.values.tolist()
    except Exception as e:
        raise LoadError(f"Failed to get dataframe values. {e}")


def store_table_in_wh(conn, query, table_rows, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} *") #seems ineffcient?
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
    """ #explore whether I can use named parameters instead
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
