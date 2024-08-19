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
        is_empty = is_bucket_empty(S3_INGEST_BUCKET)
        tables = get_table_names(conn)
        date = datetime.now().isoformat()
        latest_date = get_date(S3_INGEST_BUCKET)
        if not is_empty:
            for table_name in tables:
                copy_table(
                    S3_INGEST_BUCKET,
                    f"latest/{latest_date}/{table_name}.json",
                    f"archive/{latest_date}/{table_name}.json",
                )
                needs_update, updated_dict_table = update_dict_table(
                    S3_INGEST_BUCKET, table_name, latest_date, conn
                )
                if needs_update:
                    print(f"{table_name} table has been updated")
                    store_table_in_bucket(
                        S3_INGEST_BUCKET, updated_dict_table, table_name, date
                    )
                else:
                    copy_table(
                        S3_INGEST_BUCKET,
                        f"latest/{latest_date}/{table_name}.json",
                        f"latest/{date}/{table_name}.json",
                    )
                delete_table(
                    S3_INGEST_BUCKET, f"latest/{latest_date}/{table_name}.json"
                )
            store_date_in_bucket(S3_INGEST_BUCKET, date)
        else:
            store_date_in_bucket(S3_INGEST_BUCKET, date)
            for table_name in tables:
                dict_table = get_dict_table(conn, table_name)
                store_table_in_bucket(
                    S3_INGEST_BUCKET, dict_table, table_name, date
                )
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


def get_bucket_name():
    try:
        bucket = os.environ["S3_INGEST_BUCKET"]
        return bucket
    except KeyError as e:
        raise IngestError(f"Failed to get env bucket name. {e}")


def get_connection():
    try:
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        return pg8000.native.Connection(
            database=sm.get_secret_value(SecretId="db_name")["SecretString"],
            host=sm.get_secret_value(SecretId="db_host")["SecretString"],
            user=sm.get_secret_value(SecretId="db_user")["SecretString"],
            password=sm.get_secret_value(SecretId="db_pass")["SecretString"],
        )
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
        objects = s3.list_objects_v2(Bucket=bucket, Prefix="latest/")
        if "Contents" not in objects:
            return True
        return False
    except ClientError as e:
        raise IngestError(f"Failed to check if bucket is empty. {e}")


def delete_table(
    bucket,
    key,
    s3=boto3.client("s3", region_name="eu-west-2"),
):
    try:
        s3.delete_object(Bucket=bucket, Key=f"{key}")
    except ClientError as e:
        raise IngestError(f"Failed to delete table. {e}")


def copy_table(
    bucket,
    source_key,
    destination_key,
    s3=boto3.client("s3", region_name="eu-west-2"),
):
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": f"{source_key}"},
            Key=f"{destination_key}",
        )
    except ClientError as e:
        raise IngestError(f"Failed to copy table. {e}")


def store_table_in_bucket(
    bucket,
    dict_table,
    table_name,
    date,
    s3=boto3.client("s3", region_name="eu-west-2"),
):
    try:
        s3.put_object(
            Body=json.dumps(dict_table, indent=4, default=str).encode(),
            Bucket=bucket,
            Key=f"latest/{date}/{table_name}.json",
        )
    except ClientError as e:
        raise IngestError(f"Failed to store table in bucket. {e}")


def store_date_in_bucket(
    bucket,
    date,
    s3=boto3.client("s3", region_name="eu-west-2"),
):
    try:
        s3.put_object(
            Body=date.encode(),
            Bucket=bucket,
            Key="latest_date",
        )
    except ClientError as e:
        raise IngestError(f"Failed to store date in bucket. {e}")


def get_date(bucket, s3=boto3.client("s3", region_name="eu-west-2")):
    try:
        date_object = s3.get_object(Bucket=bucket, Key="latest_date")
        return date_object["Body"].read().decode()
    except ClientError as e:
        raise IngestError(f"Failed to get date from bucket. {e}")


def update_dict_table(
    bucket,
    table_name,
    latest_date,
    conn,
    s3=boto3.client("s3", region_name="eu-west-2"),
):
    try:
        table_object = s3.get_object(
            Bucket=bucket, Key=f"latest/{latest_date}/{table_name}.json"
        )
        dict_table = json.loads(table_object["Body"].read().decode())
        query = f"SELECT created_at FROM {table_name}"
        uningested_table_row_count = len(conn.run(query))
        ingested_table_row_count = len(dict_table["created_at"])
        row_difference = uningested_table_row_count - ingested_table_row_count
        if row_difference > 0:
            update_rows = conn.run(
                f"SELECT * FROM {table_name} OFFSET :length",
                length=ingested_table_row_count,
            )
            columns = [c["name"] for c in conn.columns]
            zipped = zip(
                columns,
                [
                    [update_row[i] for update_row in update_rows]
                    for i in range(len(columns))
                ],
            )
            for curr_zip in zipped:
                dict_table[curr_zip[0]] += curr_zip[1]
            return (True, dict_table)
        return (False, dict_table)
    except Exception as e:
        raise IngestError(f"Failed to update table. {e}")


lambda_handler("", "")
