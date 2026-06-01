import argparse
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


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


def check_flight_date(flight_date):
    try:
        datetime.strptime(flight_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("flight date must have format yyyy-mm-dd")


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


def build_source_key(flights_prefix, flight_date):
    return f"{flights_prefix}/{flight_date}/flights_{flight_date}.csv.gz"


def check_source_file(s3_client, bucket, source_key):
    logger.info("check source file: s3://%s/%s", bucket, source_key)

    try:
        response = s3_client.head_object(Bucket=bucket, Key=source_key)
    except ClientError as e:
        logger.error("source file check failed: %s", e)
        return False

    size = response.get("ContentLength", 0)
    logger.info("source file found, size: %s bytes", size)

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--flight-date",
        required=True,
        help="flight date in yyyy-mm-dd format",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="check source file without loading into postgres"
    )
    args = parser.parse_args()

    setup_logging()
    load_dotenv()

    check_flight_date(args.flight_date)

    source_bucket = get_env("SOURCE_S3_BUCKET")
    flights_prefix = get_env("FLIGHTS_PREFIX", required=False, default="flights_us_data")
    source_key = build_source_key(flights_prefix, args.flight_date)

    logger.info("source bucket: %s", source_bucket)
    logger.info("source key: %s", source_key)

    s3_client = make_s3_client()
    file_exists = check_source_file(s3_client, source_bucket, source_key)

    if not file_exists:
        raise FileNotFoundError(f"source file not found: s3://{source_bucket}/{source_key}")

    if args.dry_run:
        logger.info("dry-run mode is on")


if __name__ == "__main__":
    main()
