import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError


def create_secret(name, value):
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    try:
        sm.create_secret(Name=name, SecretString=value)
    except ClientError as e:
        sm.update_secret(SecretId=name, SecretString=value)


def store_secrets():
    load_dotenv('db.env')
    create_secret("db_name", os.environ["DB"])
    create_secret("db_host", os.environ["HOST"])
    create_secret("db_user", os.environ["DB_USER"])
    create_secret("db_pass", os.environ["PASS"])


if __name__ == "__main__":
    store_secrets()
