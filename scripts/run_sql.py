import argparse
import logging
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_env(name, required=True, default=None):
    value = os.getenv(name, default)

    if required and not value:
        raise ValueError(f"missing env variable: {name}")

    return value


def get_connection():
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


def read_sql_file(path):
    sql_path = Path(path)

    if not sql_path.exists():
        raise FileNotFoundError(f"sql file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")


def run_sql_file(path):
    sql_text = read_sql_file(path)

    logger.info("run sql file: %s", path)

    connection = get_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text)
    finally:
        connection.close()

    logger.info("sql file finished: %s", path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_file", help="sql file path")
    args = parser.parse_args()

    setup_logging()
    load_dotenv()

    run_sql_file(args.sql_file)


if __name__ == "__main__":
    main()
