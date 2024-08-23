import boto3
import os
import pg8000.native
import io
import pandas as pd
from dotenv import load_dotenv #remove in final version
from pprint import pprint


class LoadError(Exception):
    pass

def lambda_handler(event, context):
    load_dotenv() 
    S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")
    conn = get_connection()

    s3 = boto3.client('s3')
    bucket = get_bucket_name("S3_PROCESS_BUCKET")
    s3_response = s3.list_objects(
        Bucket = bucket
    )
    parquet_file_keys = [object['Key'] for object in s3_response['Contents']]

    for parquet_file_key in parquet_file_keys:
        parquet_file = get_df_from_parquet(parquet_file_key)
        #do something with this parquet file e.g. insert into data warehouse

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
    db = sm.get_secret_value(SecretId="whdb_name")["SecretString"]
    host = sm.get_secret_value(SecretId="whdb_host")["SecretString"]
    user = sm.get_secret_value(SecretId="whdb_user")["SecretString"]
    password = sm.get_secret_value(SecretId="whdb_pass")["SecretString"]
    return {"database": db, "host": host, "user": user, "password": password}

def get_connection():
    """
    Establishes a connection to the Data Warehouse using credentials from
    AWS Secrets Manager.

    Returns:
    - pg8000.native.Connection: A connection object to the database.

    """
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    return pg8000.native.Connection(**get_secrets(sm))


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



