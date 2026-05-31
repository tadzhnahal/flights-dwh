import argparse
import logging
import os

import boto3
import psycopg2
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s",)


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


def check_s3_prefix(s3_client, bucket, prefix):
    logger.info("bucket=%s prefix=%s", bucket, prefix)

    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=5,
        )
    except ClientError as e:
        logger.error("s3 check failed: %s", e)
        return False

    objects = response.get("Contents", [])

    if not objects:
        logger.warning("objects not found")
        return False

    logger.info("objects found:")
    for item in objects:
        logger.info("- %s", item["Key"])

    return True


def create_airflow_team_folder(s3_client):
    bucket = get_env("AIRFLOW_DAGS_BUCKET")
    folder = get_env("AIRFLOW_TEAM_FOLDER").strip("/")

    if not folder:
        raise ValueError("AIRFLOW_TEAM_FOLDER is empty")

    marker_key = f"{folder}/.keep"

    logger.info("create team folder marker: s3://%s/%s", bucket, marker_key)

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=marker_key,
            Body=b"",
        )
    except ClientError as e:
        logger.error("team folder create failed: %s", e)
        return

    logger.info("team folder marker is ready")


def check_airflow_ui():
    airflow_url = get_env("AIRFLOW_URL", required=False)

    if not airflow_url:
        logger.warning("airflow ui check skipped")
        logger.warning("AIRFLOW_URL is empty")
        return

    logger.info("check airflow ui: %s", airflow_url)

    try:
        response = requests.get(airflow_url, timeout=10)
    except requests.RequestException as e:
        logger.error("airflow ui check failed: %s", e)
        return

    logger.info("airflow ui status code: %s", response.status_code)


def check_postgres():
    host = get_env("POSTGRES_HOST", required=False)
    port = get_env("POSTGRES_PORT", required=False, default="5432")
    db = get_env("POSTGRES_DB", required=False)
    user = get_env("POSTGRES_USER", required=False)
    password = get_env("POSTGRES_PASSWORD", required=False)

    if not all([host, port, db, user, password]):
        logger.warning("postgres check skipped")
        logger.warning("postgres env variables are not complete")
        return

    logger.info("check postgres: %s:%s/%s", host, port, db)

    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            connect_timeout=5,
        )
    except psycopg2.Error as e:
        logger.error("postgres check failed: %s", e)
        return

    try:
        with connection.cursor() as cursor:
            cursor.execute("select current_database(), current_user;")
            row = cursor.fetchone()
            logger.info("postgres ok: database=%s user=%s", row[0], row[1])
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--create-airflow-folder",
        action="store_true",
        help="create Team_VDGA marker in airflow dags bucket",
    )
    args = parser.parse_args()

    setup_logging()
    load_dotenv()

    s3_client = make_s3_client()

    source_bucket = get_env("SOURCE_S3_BUCKET")
    flights_prefix = get_env("FLIGHTS_PREFIX", required=False, default="flights_us_data")
    airflow_bucket = get_env("AIRFLOW_DAGS_BUCKET")
    airflow_team_folder = get_env("AIRFLOW_TEAM_FOLDER", required=False, default="")

    logger.info("check source s3")
    check_s3_prefix(
        s3_client=s3_client,
        bucket=source_bucket,
        prefix=f"{flights_prefix}/",
    )

    logger.info("check airflow s3 root")
    check_s3_prefix(
        s3_client=s3_client,
        bucket=airflow_bucket,
        prefix="",
    )

    logger.info("check airflow team folder")
    check_s3_prefix(
        s3_client=s3_client,
        bucket=airflow_bucket,
        prefix=f"{airflow_team_folder.strip('/')}/",
    )

    if args.create_airflow_folder:
        create_airflow_team_folder(s3_client)

    check_airflow_ui()
    check_postgres()


if __name__ == "__main__":
    main()
