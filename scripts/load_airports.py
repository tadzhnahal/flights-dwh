import argparse
import logging
import os

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

    url = get_env("AIRPORTS_CSV_URL")
    logger.info("airports csv url: %s", url)

    if args.dry_run:
        logger.info("dry-run mode is on")


if __name__ == "__main__":
    main()
