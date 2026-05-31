import argparse
import csv
import logging
import os
from io import StringIO

import requests
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


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

    if not args.dry_run:
        logger.warning("only dry-run mode is ready now")
        logger.warning("run: python scripts/load_airports.py --dry-run")
        return

    text = download_airports_csv()
    columns, rows = read_airports(text)

    logger.info("columns: %s", columns)
    logger.info("rows: %s", len(rows))

    check_columns(columns)
    print_summary(rows)


if __name__ == "__main__":
    main()
