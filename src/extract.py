import pg8000.native
from pg8000.exceptions import DatabaseError
import boto3
from botocore.exceptions import ClientError
import os
import json
from datetime import datetime
import logging

logging.basicConfig(level=50)


class IngestError(Exception):
    pass


def lambda_handler(event, context):
    try:
        S3_INGEST_BUCKET = get_bucket_name()
        conn = get_connection()
        is_empty, keys, prefix = is_bucket_empty(S3_INGEST_BUCKET)
        tables = get_table_names(conn)
        date = format_date(datetime.now())
        if not is_empty:
            archive_tables(S3_INGEST_BUCKET, keys, prefix)
        for table in tables:
            dict_table = get_dict_table(conn, table)
            store_table_in_bucket(S3_INGEST_BUCKET, dict_table, table, date)
        return {"msg": "Ingestion successfull"}
    except IngestError as e:
        response = {"msg": "Failed to ingest data", "err": str(e)}
        logging.critical(response)
        return response
    finally:
        try:
            conn.close()
        except UnboundLocalError:
            pass


def format_date(current_time):
    return current_time.strftime('%Y-%m-%d %H:%M')


def get_bucket_name():
    try:
        bucket = os.environ["S3_INGEST_BUCKET"]
        return bucket
    except KeyError as e:
        raise IngestError(f"Failed to get env bucket name. {e}")


def get_secrets(sm):
    db = sm.get_secret_value(SecretId="db_name")["SecretString"]
    host = sm.get_secret_value(SecretId="db_host")["SecretString"]
    user = sm.get_secret_value(SecretId="db_user")["SecretString"]
    password = sm.get_secret_value(SecretId="db_pass")["SecretString"]
    return {"database": db, "host": host, "user": user, "password": password}


def get_connection():
    try:
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        return pg8000.native.Connection(**get_secrets(sm))
    except ClientError as e:
        raise IngestError(f"Failed to retrieve secrets. {e}")
    except DatabaseError as e:
        raise IngestError(f"Failed to connect to database. {e}")


def get_table_names(conn):
    try:
        tables = conn.run(
            "SELECT table_name "
            "FROM information_schema.tables "
            "WHERE table_schema='public' "
            "AND table_type='BASE TABLE';"
        )
        return [table[0] for table in tables if table[0][0] != "_"]
    except DatabaseError as e:
        raise IngestError(f"Failed to get table names. {e}")


def get_dict_table(conn, table):
    try:
        values = conn.run(f"SELECT * FROM {table}")
        columns = [c["name"] for c in conn.columns]
        return dict(
            zip(
                columns,
                [[row[i] for row in values] for i in range(len(columns))],
            )
        )
    except DatabaseError as e:
        raise IngestError(f"Failed to get table values, {e}")


def is_bucket_empty(bucket, s3=boto3.client("s3", region_name="eu-west-2")):
    try:
        prefix = "latest/"
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}")
        if "Contents" not in objects:
            return (True, [], prefix)
        keys = [obj["Key"][len(prefix):] for obj in objects["Contents"]]
        return (False, keys, prefix)
    except ClientError as e:
        raise IngestError(f"Failed to check if bucket is empty. {e}")


def archive_tables(
    bucket, keys, prefix, s3=boto3.client("s3", region_name="eu-west-2")
):
    try:
        for key in keys:
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": f"{prefix}{key}"},
                Key=f"archive/{key}",
            )
            s3.delete_object(Bucket=bucket, Key=f"{prefix}{key}")
    except ClientError as e:
        raise IngestError(f"Failed to archive tables. {e}")


def store_table_in_bucket(
    bucket,
    dict_table,
    table_name,
    date,
    s3=boto3.client("s3", region_name="eu-west-2")
):
    try:
        s3.put_object(
            Body=json.dumps(dict_table, indent=4, default=str).encode(),
            Bucket=bucket,
            Key=f"latest/{date}/{table_name}.json",
        )
    except ClientError as e:
        raise IngestError(f"Failed to store table in bucket. {e}")
