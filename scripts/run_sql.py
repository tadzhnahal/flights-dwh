import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from common import get_postgres_connection, setup_logging


logger = logging.getLogger(__name__)


def read_sql_file(path):
    sql_path = Path(path)

    if not sql_path.exists():
        raise FileNotFoundError(f"sql file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")


def run_sql_file(path):
    sql_text = read_sql_file(path)

    logger.info("run sql file: %s", path)

    connection = get_postgres_connection()

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
