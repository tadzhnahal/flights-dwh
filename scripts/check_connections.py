import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


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
    print(f"bucket: {bucket}, prefix: {prefix}")

    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=5,
        )
    except ClientError as e:
        print(f"s3 check failed: {e}")
        return

    objects = response.get("Contents", [])

    if not objects:
        print("objects not found")
        return

    print("objects found:")
    for item in objects:
        print(f"- {item['Key']}")


def main():
    load_dotenv()

    s3_client = make_s3_client()

    source_bucket = get_env("SOURCE_S3_BUCKET")
    flights_prefix = get_env("FLIGHTS_PREFIX", required=False, default="flights_us_data")
    airflow_bucket = get_env("AIRFLOW_DAGS_BUCKET")

    print("check source s3")
    check_s3_prefix(
        s3_client=s3_client,
        bucket=source_bucket,
        prefix=f"{flights_prefix}/",
    )

    print()
    print("check airflow s3")
    check_s3_prefix(
        s3_client=s3_client,
        bucket=airflow_bucket,
        prefix="",
    )


if __name__ == "__main__":
    main()
