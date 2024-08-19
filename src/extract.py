import pg8000.native
from pg8000.exceptions import DatabaseError
import boto3
from botocore.exceptions import ClientError
import os
import json
from datetime import datetime
import logging


logging.basicConfig(level=50)
"""
Configures the logging module to log only critical messages.
"""

class IngestError(Exception):
    pass
"""
Catch-all Error to make our lambda_handler function shorter and more functional
"""

def lambda_handler(event, context):
    """
        Handles the data ingestion process for tables from Totesys Database to S3 bucket.

        This function is designed to be used as an AWS Lambda handler. It performs the following tasks:
        - Establishes a connection to a database.
        - Checks if the specified S3 bucket is empty.
        - Retrieves the tables from the database.
        - Manages data updates, archiving, and storage in the S3 bucket.

        Workflow:
        - Retrieves the S3 bucket name and checks if it is empty.
        - If the bucket is not empty, it copies existing data to an archive location, checks if any tables need updating, and updates them accordingly.
        - If the bucket is empty, it stores the current date and ingests the latest data from the database tables into the S3 bucket.
        - Stores the ingestion date in the bucket for reference.

        Returns:
        - A message indicating the success or failure of the ingestion process.

        Error Handling:
        - If an `IngestError` occurs, it logs the error as a critical issue and returns a failure message.
        - Ensures that the database connection is closed, even if an error occurs during execution.

        Example Usage:
        - This function is intended to be deployed in an AWS Lambda environment and triggered by an EventBridge event that starts the ingestion process.
    """
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
    """
    Retrieves the name of the S3 bucket used for ingestion.

    Returns:
    - The name of the S3 bucket as a string.

    Raises:
    - IngestError: If the environment variable 'S3_INGEST_BUCKET' is not found.
    """
    try:
        bucket = os.environ["S3_INGEST_BUCKET"]
        return bucket
    except KeyError as e:
        raise IngestError(f"Failed to get env bucket name. {e}")


def get_secrets(sm):
    """
    Retrieves database connection details from AWS Secrets Manager.

    Parameters:
    - sm (boto3.client): The Secrets Manager client.

    Returns:
    - A dictionary containing database credentials including:
        - 'database','host','user','password'
    """
    db = sm.get_secret_value(SecretId="db_name")["SecretString"]
    host = sm.get_secret_value(SecretId="db_host")["SecretString"]
    user = sm.get_secret_value(SecretId="db_user")["SecretString"]
    password = sm.get_secret_value(SecretId="db_pass")["SecretString"]
    return {"database": db, "host": host, "user": user, "password": password}


def get_connection():
    """
    Establishes a connection to the ToteSys database using credentials from AWS Secrets Manager.

    Returns:
    - pg8000.native.Connection: A connection object to the database.

    Raises:
    - IngestError: If there is an issue retrieving secrets or connecting to the database.
    """
    try:
        sm = boto3.client("secretsmanager", region_name="eu-west-2")
        return pg8000.native.Connection(**get_secrets(sm))
    except ClientError as e:
        raise IngestError(f"Failed to retrieve secrets. {e}")
    except DatabaseError as e:
        raise IngestError(f"Failed to connect to database. {e}")


def get_table_names(conn):
    """
    Retrieves the names of all tables in the ToteSys database.

    Parameters:
    - conn (pg8000.native.Connection): The database connection object.

    Returns:
    - A list of table names that are not prefixed with an underscore.

    Raises:
    - IngestError: If there is an issue executing the query to retrieve table names.
    """
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
    """
    Retrieves all rows from a specified table and converts them into a dictionary format.

    Parameters:
    - conn (pg8000.native.Connection): The database connection object.
    - table (str): The name of the ToteSys Database table to retrieve data from.

    Returns:
    - dict: A dictionary where the keys are column names and the values are lists of column data.

    Raises:
    - IngestError: If there is an issue executing the query to retrieve table data.
    """
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
    """
    Checks whether the S3 bucket is empty.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - s3 (boto3.client, optional): The S3 client. Defaults to a client for the 'eu-west-2' region.

    Returns:
    - True if the bucket is empty, False if bucket is not empty.

    Raises:
    - IngestError: If there is an issue accessing the S3 bucket.
    """
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
    """
    Deletes a specified table from the S3 bucket.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - key (str): The key of the table to delete.
    - s3 (boto3.client, optional): The S3 client.

    Raises:
    - IngestError: If there is an issue deleting the object from the S3 bucket.
    """
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
    """
    Copies a table from latest folder to archive folder within the S3 bucket.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - source_key (str): The key of latest table.
    - destination_key (str): The key of the archive folder.
    - s3 (boto3.client, optional): The S3 client. Defaults to a client for the 'eu-west-2' region.

    Raises:
    - IngestError: If there is an issue copying the object within the S3 bucket.
    """
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
    """
    Stores a table (in dictionary format) in the S3 bucket in the 'latest' folder with the current date.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - dict_table (dict): The table data in dictionary format.
    - table_name (str): The name of the table.
    - date (str): The current date to be used in the key.
    - s3 (boto3.client, optional): The S3 client. Defaults to a client for the 'eu-west-2' region.

    Raises:
    - IngestError: If there is an issue storing the table in the S3 bucket.
    """
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
    """
    Stores the current date in the S3 bucket under the key 'latest_date'.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - date (str): The current date to store.
    - s3 (boto3.client, optional): The S3 client.

    Raises:
    - IngestError: If there is an issue storing the date in the S3 bucket.
    """
    try:
        s3.put_object(
            Body=date.encode(),
            Bucket=bucket,
            Key="latest_date",
        )
    except ClientError as e:
        raise IngestError(f"Failed to store date in bucket. {e}")


def get_date(bucket, s3=boto3.client("s3", region_name="eu-west-2")):
    """
    Retrieves the latest ingestion date from the S3 bucket.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - s3 (boto3.client, optional): The S3 client. 

    Returns:
    - str: The latest ingestion date as a string.

    Raises:
    - IngestError: If there is an issue retrieving the date from the S3 bucket.
    """
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
    """
    Updates a dictionary table with new rows from the database, if any are available.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - table_name (str): The name of the table to update.
    - latest_date (str): The date of the last ingestion.
    - conn (pg8000.native.Connection): The database connection object.
    - s3 (boto3.client, optional): The S3 client. 

    Returns:
    - A tuple containing:
        - bool: True if the table was updated, False if not.
        - dict: The updated table in dictionary format.

    Raises:
    - IngestError: If there is an issue retrieving or updating the table.
    """
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
