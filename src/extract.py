import pg8000.native
import boto3
import os
import json

S3_INGESTION_BUCKET = ""



def lambda_handler(event, context):
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    conn = pg8000.native.Connection(
        database=sm.get_secret_value(SecretId="db_name")["SecretString"],
        host=sm.get_secret_value(SecretId="db_host")["SecretString"],
        user=sm.get_secret_value(SecretId="db_user")["SecretString"],
        password=sm.get_secret_value(SecretId="db_pass")["SecretString"],
    )
    s3 = boto3.client("s3", region_name="eu-west-2")
    tables = conn.run(
        "SELECT table_name "
        "FROM information_schema.tables "
        "WHERE table_schema='public' "
        "AND table_type='BASE TABLE';"
    )
    dict_tables = []
    for table in tables:
        values = conn.run(f"SELECT * FROM {table[0]}")
        columns = [c["name"] for c in conn.columns]
        dict_tables.append(
            dict(
                zip(columns, [[row[i] for row in values] for i in range(len(columns))])
            )
        )
