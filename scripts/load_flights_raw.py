import argparse
import logging
import os
from datetime import datetime
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
        raise ValueError(f"flight date must have format yyyy-mm-dd")


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

    logger.info("source bucket: %s", source_bucket)
    logger.info("flights_prefix: %s", flights_prefix)
    logger.info("flight date: %s", args.flight_date)

    if args.dry_run:
        logger.info("dry-run mode is on")


if __name__ == "__main__":
    main()
