import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import logging
import io
import os

logging.basicConfig(level=50)

class ProcessError(Exception):
    pass

def lambda_handler(event, context):
    try:
        global S3_INGEST_BUCKET, S3_PROCESS_BUCKET
        S3_INGEST_BUCKET = get_bucket_name("S3_INGEST_BUCKET")
        S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")

        processing_functions = {
            "staff": process_staff,
        }

        processed_table_names = [processing_functions[table_name]() for table_name in event["tables"]]

        return {"msg": "Data process successful.", "tables": sorted(processed_table_names)}

    except ProcessError as e:
        logging.critical(e)
        return {"msg": "Failed to process data", "err": str(e)}

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



def process_staff():
    try:
        df_staff = get_dataframe_from_table_json(S3_INGEST_BUCKET, "staff")
        df_department = get_dataframe_from_table_json(S3_INGEST_BUCKET, "department")
        dim_staff = get_dim_staff(df_staff, df_department)
        dim_staff_parquet = df_to_parquet(dim_staff)
        store_parquet_file(S3_PROCESS_BUCKET, dim_staff_parquet, "dim_staff")
        return "dim_staff"
    except ProcessError as e:
        logging.critical(e)
        return None
    
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