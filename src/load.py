import boto3
import os
import pg8000.native

def lambda_handler(event, context):
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