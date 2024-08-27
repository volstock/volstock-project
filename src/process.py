import boto3
import pandas as pd
from botocore.exceptions import ClientError
import pg8000.dbapi
from pg8000.exceptions import DatabaseError
import logging
import os
import io
import json

logging.basicConfig(level=logging.WARNING)

class LoadError(Exception):
    pass

class ProcessError(Exception):
    pass

def lambda_handler(event, context):
    #recipe = table to update + pandas function to remodel into s3 bucket
    
    try:
        S3_INGEST_BUCKET = get_bucket_name("S3_INGEST_BUCKET")
        S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")
        conn = get_connection()
        update_tables_names = []

        table_functions = {
            "staff": process_dim_staff,
            "address": process_dim_location,
            "design": process_dim_design,
            "currency": process_dim_currency,
            "counterparty": process_dim_counterparty,
            "sales_order": process_fact_sales_order,
            "transaction": process_dim_transaction,
            "payment_type": process_dim_payment_type,
            "payment": process_fact_payment,
            "purchase_order": process_fact_purchase_order,
        }

        for table_name in event["tables"]:
            if table_name in table_functions:
                table_functions[table_name](S3_INGEST_BUCKET, S3_PROCESS_BUCKET)
                processed_table_name = f"dim_{table_name}" if table_name in get_list_of_dim_tables() else f"fact_{table_name}"
                update_tables_names.append(processed_table_name)
                store_table_in_warehouse(conn, processed_table_name) #this part is wrong. store in s3 bucket instead
            else:
                logging.warning(f"Unknown table {table_name}")

        if "fact_sales_order" in update_tables_names:
            process_dim_date(S3_PROCESS_BUCKET, "dim_date", update_tables_names)
            store_table_in_warehouse(conn, "dim_date")

        return {"msg": "Data process successful.", "tables": update_tables_names}
    except (LoadError, ProcessError) as e:
        logging.critical(e)
        return {"msg": "Failed to process or load data", "err": str(e)}

def get_bucket_name(bucket_env_var):
    try:
        return os.environ[bucket_env_var]
    except KeyError as e:
        raise LoadError(f"Failed to get env bucket name. {e}")

def get_connection():
    try:
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        secrets = get_secrets(sm)
        return pg8000.dbapi.connect(**secrets)
    except DatabaseError as e:
        raise LoadError(f"Failed to get connection. {e}")

def get_secrets(sm):
    try:
        return {
            "database": sm.get_secret_value(SecretId="whdb_name")["SecretString"],
            "host": sm.get_secret_value(SecretId="whdb_host")["SecretString"],
            "user": sm.get_secret_value(SecretId="whdb_user")["SecretString"],
            "password": sm.get_secret_value(SecretId="whdb_pass")["SecretString"]
        }
    except ClientError as e:
        raise LoadError(f"Failed to get secrets. {e}")

def get_dataframe_from_table_json(bucket, table_name):
    s3 = boto3.client("s3", region_name="eu-west-2")
    latest_date = get_latest_date(bucket)
    obj = s3.get_object(Bucket=bucket, Key=f"latest/{latest_date}/{table_name}.json")
    return pd.read_json(obj['Body'])

def get_latest_date(bucket):
    s3 = boto3.client("s3", region_name="eu-west-2")
    response = s3.list_objects_v2(Bucket=bucket, Prefix="latest/", Delimiter="/")
    dates = [obj['Prefix'].split('/')[1] for obj in response.get('CommonPrefixes', [])]
    return max(dates) if dates else None

def store_parquet_file(bucket, parquet_file, file_name):
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.put_object(Bucket=bucket, Key=f"{file_name}.parquet", Body=parquet_file)

def df_to_parquet(df):
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    return parquet_buffer

def store_table_in_warehouse(conn, table_name):
    try:
        df = get_dataframe_from_parquet(os.environ["S3_PROCESS_BUCKET"], table_name)
        query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} *")
        cursor.executemany(query, df.values.tolist())
        conn.commit()
    except DatabaseError as e:
        raise LoadError(f"Failed to store {table_name} in warehouse db. {e}")

def get_dataframe_from_parquet(bucket, parquet_name):
    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        buffer = io.BytesIO()
        s3.download_fileobj(Bucket=bucket, Key=f"{parquet_name}.parquet", Fileobj=buffer)
        return pd.read_parquet(buffer).convert_dtypes().replace({pd.NA: None})
    except ClientError as e:
        raise LoadError(f"Failed to get dataframe from parquet file. {e}")

def get_list_of_dim_tables():
    return ["staff", "address", "design", "currency", "counterparty", "transaction", "payment_type"]

def process_dim_staff(s3_ingest_bucket, s3_process_bucket):
    df_staff = get_dataframe_from_table_json(s3_ingest_bucket, "staff")
    df_department = get_dataframe_from_table_json(s3_ingest_bucket, "department")
    df_staff_department = df_staff.merge(df_department, on="department_id")
    processed_df = df_staff_department[["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_staff")

def process_dim_location(s3_ingest_bucket, s3_process_bucket):
    df = get_dataframe_from_table_json(s3_ingest_bucket, "address")
    processed_df = df.rename(columns={"address_id": "location_id"}).drop(columns=["created_at", "last_updated"])
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_location")

def process_dim_design(s3_ingest_bucket, s3_process_bucket):
    df = get_dataframe_from_table_json(s3_ingest_bucket, "design")
    processed_df = df.drop(columns=["created_at", "last_updated"])
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_design")

def process_dim_currency(s3_ingest_bucket, s3_process_bucket):
    df_currency = get_dataframe_from_table_json(s3_ingest_bucket, "currency")
    df_currency_names = get_currency_names_dataframe()
    df_currency["currency_code"] = df_currency["currency_code"].str.lower()
    processed_df = df_currency.merge(df_currency_names, on="currency_code").drop(columns=["created_at", "last_updated"])
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_currency")

def process_dim_counterparty(s3_ingest_bucket, s3_process_bucket):
    df_counterparty = get_dataframe_from_table_json(s3_ingest_bucket, "counterparty")
    df_address = get_dataframe_from_table_json(s3_ingest_bucket, "address")
    df_combined = df_counterparty.merge(df_address, left_on="legal_address_id", right_on="address_id")
    processed_df = df_combined.rename(columns={
        "address_line_1": "counterparty_legal_address_line_1",
        "address_line_2": "counterparty_legal_address_line_2",
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone_number",
    })[["counterparty_id", "counterparty_legal_name", "counterparty_legal_address_line_1", "counterparty_legal_address_line_2", "counterparty_legal_district", "counterparty_legal_city", "counterparty_legal_postal_code", "counterparty_legal_country", "counterparty_legal_phone_number"]]
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_counterparty")

def process_dim_transaction(s3_ingest_bucket, s3_process_bucket):
    df = get_dataframe_from_table_json(s3_ingest_bucket, "transaction")
    processed_df = df.drop(columns=["created_at", "last_updated"])
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_transaction")

def process_dim_payment_type(s3_ingest_bucket, s3_process_bucket):
    df = get_dataframe_from_table_json(s3_ingest_bucket, "payment_type")
    processed_df = df.drop(columns=["created_at", "last_updated"])
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "dim_payment_type")

def process_fact_payment(s3_ingest_bucket, s3_process_bucket):
    df_payment = get_dataframe_from_table_json(s3_ingest_bucket, "payment")
    df_payment["payment_record_id"] = range(1, len(df_payment) + 1)
    df_payment["created_date"] = pd.to_datetime(df_payment["created_at"]).dt.date
    df_payment["created_time"] = pd.to_datetime(df_payment["created_at"]).dt.time
    df_payment["last_updated_date"] = pd.to_datetime(df_payment["last_updated"]).dt.date
    df_payment["last_updated_time"] = pd.to_datetime(df_payment["last_updated"]).dt.time
    processed_df = df_payment.drop(columns=["created_at", "last_updated"]).set_index("payment_record_id")
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "fact_payment")

def process_fact_purchase_order(s3_ingest_bucket, s3_process_bucket):
    df_purchase_order = get_dataframe_from_table_json(s3_ingest_bucket, "purchase_order")
    df_purchase_order["purchase_record_id"] = range(1, len(df_purchase_order) + 1)
    df_purchase_order["created_date"] = pd.to_datetime(df_purchase_order["created_at"]).dt.date
    df_purchase_order["created_time"] = pd.to_datetime(df_purchase_order["created_at"]).dt.time
    df_purchase_order["last_updated_date"] = pd.to_datetime(df_purchase_order["last_updated"]).dt.date
    df_purchase_order["last_updated_time"] = pd.to_datetime(df_purchase_order["last_updated"]).dt.time
    processed_df = df_purchase_order.drop(columns=["created_at", "last_updated"]).set_index("purchase_record_id")
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "fact_purchase_order")

def process_fact_sales_order(s3_ingest_bucket, s3_process_bucket):
    df_sales_order = get_dataframe_from_table_json(s3_ingest_bucket, "sales_order")
    df_sales_order["sales_record_id"] = range(1, len(df_sales_order) + 1)
    df_sales_order["created_date"] = pd.to_datetime(df_sales_order["created_at"]).dt.date
    df_sales_order["created_time"] = pd.to_datetime(df_sales_order["created_at"]).dt.time
    df_sales_order["last_updated_date"] = pd.to_datetime(df_sales_order["last_updated"]).dt.date
    df_sales_order["last_updated_time"] = pd.to_datetime(df_sales_order["last_updated"]).dt.time
    processed_df = df_sales_order.rename(columns={"staff_id": "sales_staff_id"}).drop(columns=["created_at", "last_updated"]).set_index("sales_record_id")
    store_parquet_file(s3_process_bucket, df_to_parquet(processed_df), "fact_sales_order")

def process_dim_date(s3_process_bucket, parquet_name, update_tables_names):
    df_fact_sales_order = get_dataframe_from_parquet(s3_process_bucket, "fact_sales_order")
    timestamps = pd.to_datetime(pd.concat([df_fact_sales_order["created_date"], df_fact_sales_order["last_updated_date"], df_fact_sales_order["agreed_payment_date"], df_fact_sales_order["agreed_delivery_date"]]).drop_duplicates())
    dim_date = pd.DataFrame({"date_id": timestamps}).assign(
        year=timestamps.dt.year,
        month=timestamps.dt.month,
        day=timestamps.dt.day,
        day_of_week=timestamps.dt.dayofweek,
        day_name=timestamps.dt.day_name(),
        month_name=timestamps.dt.month_name(),
        quarter=timestamps.dt.quarter
    )
    store_parquet_file(s3_process_bucket, df_to_parquet(dim_date), parquet_name)
    update_tables_names.append(parquet_name)

def get_currency_names_dataframe():
    # This function should return a DataFrame with currency codes and names
    # You might want to implement this based on your specific requirements
    pass