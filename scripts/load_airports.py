import argparse
import csv
import logging
from datetime import datetime, timezone
from io import StringIO

import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from cleaning import clean_float, clean_int, clean_text
from common import get_env, get_postgres_connection, setup_logging


logger = logging.getLogger(__name__)


TARGET_TABLE = "team_vdga_stg.airports"


AIRPORT_COLUMNS = [
    "id",
    "ident",
    "type",
    "name",
    "latitude_deg",
    "longitude_deg",
    "elevation_ft",
    "continent",
    "iso_country",
    "iso_region",
    "municipality",
    "scheduled_service",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code",
    "home_link",
    "wikipedia_link",
    "keywords",
]


INSERT_COLUMNS = [
    "airport_id",
    "ident",
    "type",
    "name",
    "latitude_deg",
    "longitude_deg",
    "elevation_ft",
    "continent",
    "iso_country",
    "iso_region",
    "municipality",
    "scheduled_service",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code",
    "home_link",
    "wikipedia_link",
    "keywords",
    "timezone",
    "loaded_at",
]


def download_airports_csv():
    url = get_env("AIRPORTS_CSV_URL")

    logger.info("download airports csv: %s", url)

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    return response.text


def read_airports(text):
    reader = csv.DictReader(StringIO(text))
    rows = list(reader)

    return reader.fieldnames, rows


def check_columns(columns):
    missing_columns = []

    for column in AIRPORT_COLUMNS:
        if column not in columns:
            missing_columns.append(column)

    if missing_columns:
        raise ValueError(f"missing columns: {missing_columns}")

    if "timezone" not in columns:
        logger.warning("timezone column not found in airports.csv")
        logger.warning("timezone will be null in team_vdga_stg.airports")


def prepare_airport_rows(rows):
    prepared_rows = []
    loaded_at = datetime.now(timezone.utc)

    for row in rows:
        prepared_rows.append(
            (
                clean_int(row.get("id")),
                clean_text(row.get("ident")),
                clean_text(row.get("type")),
                clean_text(row.get("name")),
                clean_float(row.get("latitude_deg")),
                clean_float(row.get("longitude_deg")),
                clean_int(row.get("elevation_ft")),
                clean_text(row.get("continent")),
                clean_text(row.get("iso_country")),
                clean_text(row.get("iso_region")),
                clean_text(row.get("municipality")),
                clean_text(row.get("scheduled_service")),
                clean_text(row.get("icao_code")),
                clean_text(row.get("iata_code")),
                clean_text(row.get("gps_code")),
                clean_text(row.get("local_code")),
                clean_text(row.get("home_link")),
                clean_text(row.get("wikipedia_link")),
                clean_text(row.get("keywords")),
                None,
                loaded_at,
            )
        )

    return prepared_rows


def print_summary(rows):
    iata_count = 0

    for row in rows:
        if row.get("iata_code"):
            iata_count += 1

    logger.info("rows with iata_code: %s", iata_count)

    logger.info("sample rows:")
    for row in rows[:5]:
        logger.info(
            "id=%s ident=%s type=%s name=%s iata_code=%s",
            row.get("id"),
            row.get("ident"),
            row.get("type"),
            row.get("name"),
            row.get("iata_code"),
        )


def load_airports_to_postgres(prepared_rows):
    columns_sql = ", ".join(INSERT_COLUMNS)

    sql = f"""
        insert into {TARGET_TABLE} ({columns_sql})
        values %s
    """

    logger.info("load airports into %s", TARGET_TABLE)

    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                execute_values(cursor, sql, prepared_rows, page_size=1000)
    finally:
        connection.close()

    logger.info("airports loaded: %s", len(prepared_rows))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="check airports csv without loading into postgres",
    )
    args = parser.parse_args()

    setup_logging()
    load_dotenv()

    text = download_airports_csv()
    columns, rows = read_airports(text)

    logger.info("columns: %s", columns)
    logger.info("rows: %s", len(rows))

    check_columns(columns)

    prepared_rows = prepare_airport_rows(rows)
    logger.info("prepared rows: %s", len(prepared_rows))

    print_summary(rows)

    if args.dry_run:
        logger.info("dry-run finished")
        return

    load_airports_to_postgres(prepared_rows)


if __name__ == "__main__":
    main()
