import logging
import os

import boto3
import psycopg2


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def get_env(name, required=True, default=None):
    value = os.getenv(name, default)

    if required and not value:
        raise ValueError(f"missing env variable: {name}")

    return value


def make_s3_client():
    endpoint_url = get_env("S3_ENDPOINT_URL")
    access_key = get_env("AWS_ACCESS_KEY_ID")
    secret_key = get_env("AWS_SECRET_ACCESS_KEY")
    region = get_env("AWS_DEFAULT_REGION", required=False, default="ru-central1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def get_postgres_connection():
    host = get_env("POSTGRES_HOST")
    port = get_env("POSTGRES_PORT")
    db = get_env("POSTGRES_DB")
    username = get_env("POSTGRES_USER")
    password = get_env("POSTGRES_PASSWORD")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=username,
        password=password,
        connect_timeout=5,
    )
