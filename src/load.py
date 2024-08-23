import boto3
import os
import pg8000.native
import io
import pandas as pd
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

class LoadError(Exception):
    pass

def lambda_handler(event, context):
    S3_PROCESS_BUCKET = get_bucket_name("S3_PROCESS_BUCKET")
    conn = get_connection()
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'project_team_4'
    """
    table_names = conn.run(query)
    print("Tables in the database:")
    for table in table_names:
        print(table[0])

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
    db = sm.get_secret_value(SecretId="wh_name_")["SecretString"]
    host = sm.get_secret_value(SecretId="wh_host_")["SecretString"]
    user = sm.get_secret_value(SecretId="wh_user_")["SecretString"]
    password = sm.get_secret_value(SecretId="wh_pass_")["SecretString"]
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


def get_df_from_parquet():
    buffer = io.BytesIO()
    s3 = boto3.client('s3')
    bucket = get_bucket_name("S3_PROCESS_BUCKET")
    response = s3.list_objects(
        Bucket = bucket
    )
    answer = []
    for object in response['Contents']:
        answer.append(object['Key'])
    print(answer)

