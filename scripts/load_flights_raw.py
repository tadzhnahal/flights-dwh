import argparse
import csv
import gzip
import logging
import os
from datetime import datetime
from io import BytesIO, TextIOWrapper

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


target_table = "team_vdga_stg.flights_raw"


required_columns = [
    "flightdate",
    "reporting_airline",
    "tail_number",
    "flight_number_reporting_airline",
    "origin",
    "dest",
    "distance",
    "crsdeptime",
    "deptime",
    "depdelayminutes",
    "crsarrtime",
    "arrtime",
    "arrdelayminutes",
    "taxiout",
    "wheelsoff",
    "wheelson",
    "taxiin",
    "carrierdelay",
    "weatherdelay",
    "nasdelay",
    "securitydelay",
    "lateaircraftdelay",
    "cancelled",
    "cancellationcode",
]


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
        response = s3_client.head_object(
            Bucket=bucket,
            Key=source_key,
        )
    except ClientError as error:
        logger.error("source file check failed: %s", error)
        return False

    size = response.get("ContentLength", 0)
    logger.info("source file found, size: %s bytes", size)

    return True


def download_source_file(s3_client, bucket, source_key):
    logger.info("download source file")

    response = s3_client.get_object(
        Bucket=bucket,
        Key=source_key,
    )

    return response["Body"].read()


def normalize_column_name(name):
    return name.strip().lower()


def normalize_row(row):
    normalized_row = {}

    for key, value in row.items():
        normalized_key = normalize_column_name(key)
        normalized_row[normalized_key] = value

    return normalized_row


def read_flights_gz(file_bytes):
    sample_rows = []
    row_count = 0
    columns = []

    with gzip.GzipFile(fileobj=BytesIO(file_bytes)) as gzip_file:
        text_file = TextIOWrapper(gzip_file, encoding="utf-8-sig")
        reader = csv.DictReader(text_file)

        if reader.fieldnames:
            for column in reader.fieldnames:
                columns.append(normalize_column_name(column))

        for row in reader:
            normalized_row = normalize_row(row)
            row_count += 1

            if len(sample_rows) < 5:
                sample_rows.append(normalized_row)

    return columns, row_count, sample_rows


def check_columns(columns):
    missing_columns = []

    for column in required_columns:
        if column not in columns:
            missing_columns.append(column)

    if missing_columns:
        raise ValueError(f"missing columns: {missing_columns}")

    logger.info("required columns found")


def clean_text(value):
    if value == "":
        return None

    return value


def clean_float(value):
    if value == "":
        return None

    return float(value)


def clean_bool(value):
    if value == "":
        return None

    return float(value) == 1.0


def get_year_month(flight_dt):
    date_value = datetime.strptime(flight_dt, "%Y-%m-%d")

    return date_value.year, date_value.month


def prepare_preview_rows(sample_rows, source_key):
    prepared_rows = []

    for row in sample_rows:
        year, month = get_year_month(row.get("flightdate"))

        prepared_rows.append(
            {
                "year": year,
                "month": month,
                "flight_dt": row.get("flightdate"),
                "carrier_code": clean_text(row.get("reporting_airline")),
                "tail_num": clean_text(row.get("tail_number")),
                "carrier_flight_number": clean_text(row.get("flight_number_reporting_airline")),
                "origin_code": clean_text(row.get("origin")),
                "dest_code": clean_text(row.get("dest")),
                "distance": clean_float(row.get("distance")),
                "scheduled_dep_tm": clean_text(row.get("crsdeptime")),
                "actual_dep_tm": clean_text(row.get("deptime")),
                "dep_delay_min": clean_float(row.get("depdelayminutes")),
                "scheduled_arr_tm": clean_text(row.get("crsarrtime")),
                "actual_arr_tm": clean_text(row.get("arrtime")),
                "arr_delay_min": clean_float(row.get("arrdelayminutes")),
                "taxi_out_min": clean_float(row.get("taxiout")),
                "wheels_off_tm": clean_text(row.get("wheelsoff")),
                "wheels_on_tm": clean_text(row.get("wheelson")),
                "taxi_in_min": clean_float(row.get("taxiin")),
                "carrier_delay_min": clean_float(row.get("carrierdelay")),
                "weather_delay_min": clean_float(row.get("weatherdelay")),
                "nas_delay_min": clean_float(row.get("nasdelay")),
                "security_delay_min": clean_float(row.get("securitydelay")),
                "late_aircraft_min": clean_float(row.get("lateaircraftdelay")),
                "cancelled": clean_bool(row.get("cancelled")),
                "cancellation_code": clean_text(row.get("cancellationcode")),
                "source_file": source_key,
            }
        )

    return prepared_rows


def warn_if_dates_differ(folder_date, sample_rows):
    flight_dates = set()

    for row in sample_rows:
        flight_date = row.get("flightdate")

        if flight_date:
            flight_dates.add(flight_date)

    if flight_dates and folder_date not in flight_dates:
        logger.warning(
            "folder date differs from flightdate in sample: folder=%s sample=%s",
            folder_date,
            sorted(flight_dates),
        )


def print_summary(columns, row_count, sample_rows):
    logger.info("columns: %s", columns)
    logger.info("rows: %s", row_count)

    logger.info("sample rows:")
    for row in sample_rows:
        logger.info(
            "flightdate=%s carrier=%s origin=%s dest=%s dep_delay=%s arr_delay=%s",
            row.get("flightdate"),
            row.get("reporting_airline"),
            row.get("origin"),
            row.get("dest"),
            row.get("depdelayminutes"),
            row.get("arrdelayminutes"),
        )


def print_prepared_preview(prepared_rows):
    logger.info("target table: %s", target_table)
    logger.info("prepared preview:")

    for row in prepared_rows:
        logger.info(
            "flight_dt=%s carrier=%s origin=%s dest=%s dep_delay=%s arr_delay=%s source_file=%s",
            row.get("flight_dt"),
            row.get("carrier_code"),
            row.get("origin_code"),
            row.get("dest_code"),
            row.get("dep_delay_min"),
            row.get("arr_delay_min"),
            row.get("source_file"),
        )


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
        help="check source file without loading into postgres",
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

    file_bytes = download_source_file(s3_client, source_bucket, source_key)
    columns, row_count, sample_rows = read_flights_gz(file_bytes)

    check_columns(columns)
    print_summary(columns, row_count, sample_rows)
    warn_if_dates_differ(args.flight_date, sample_rows)

    prepared_rows = prepare_preview_rows(sample_rows, source_key)
    print_prepared_preview(prepared_rows)

    if args.dry_run:
        logger.info("dry-run finished")


if __name__ == "__main__":
    main()
