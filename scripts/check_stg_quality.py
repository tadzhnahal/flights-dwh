import argparse
import logging
from datetime import datetime

from dotenv import load_dotenv

from common import get_postgres_connection, setup_logging


logger = logging.getLogger(__name__)


AIRPORTS_TABLE = "team_vdga_stg.airports"
FLIGHTS_TABLE = "team_vdga_stg.flights_raw"
LOG_TABLE = "team_vdga_metadata.etl_load_log"
FLOW_NAME = "stg_flights_raw"


def check_flight_date(flight_date):
    try:
        datetime.strptime(flight_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("flight date must have format yyyy-mm-dd")


def build_source_key(flights_prefix, flight_date):
    return f"{flights_prefix}/{flight_date}/flights_{flight_date}.csv.gz"


def fetch_one(cursor, sql, params=None):
    if params is None:
        params = ()

    cursor.execute(sql, params)

    return cursor.fetchone()


def check_airports(cursor):
    sql = f"""
        select
            count(*) as rows_cnt,
            count(*) filter (where iata_code is not null) as rows_with_iata
        from {AIRPORTS_TABLE};
    """

    rows_cnt, rows_with_iata = fetch_one(cursor, sql)

    logger.info("airports rows: %s", rows_cnt)
    logger.info("airports rows with iata_code: %s", rows_with_iata)

    if rows_cnt == 0:
        raise ValueError("airports table is empty")

    if rows_with_iata == 0:
        raise ValueError("airports table has no iata_code values")


def check_flights_raw(cursor, source_file):
    sql = f"""
        select
            count(*) as rows_cnt,
            count(*) filter (where flight_dt is null) as null_flight_dt_cnt,
            count(*) filter (where source_file is null) as null_source_file_cnt,
            min(flight_dt) as min_flight_dt,
            max(flight_dt) as max_flight_dt
        from {FLIGHTS_TABLE}
        where source_file = %s;
    """

    row = fetch_one(cursor, sql, (source_file,))

    rows_cnt = row[0]
    null_flight_dt_cnt = row[1]
    null_source_file_cnt = row[2]
    min_flight_dt = row[3]
    max_flight_dt = row[4]

    logger.info("flights source_file: %s", source_file)
    logger.info("flights rows: %s", rows_cnt)
    logger.info("flights null flight_dt: %s", null_flight_dt_cnt)
    logger.info("flights null source_file: %s", null_source_file_cnt)
    logger.info("flights min flight_dt: %s", min_flight_dt)
    logger.info("flights max flight_dt: %s", max_flight_dt)

    if rows_cnt == 0:
        raise ValueError("flights_raw has no rows for source_file")

    if null_flight_dt_cnt > 0:
        raise ValueError("flights_raw has empty flight_dt")

    if null_source_file_cnt > 0:
        raise ValueError("flights_raw has empty source_file")

    return rows_cnt


def check_load_log(cursor, source_file, rows_cnt):
    success_count_sql = f"""
        select count(*)
        from {LOG_TABLE}
        where flow_name = %s
          and source_file = %s
          and status = 'success';
    """

    success_count = fetch_one(cursor, success_count_sql, (FLOW_NAME, source_file))[0]

    logger.info("success log rows: %s", success_count)

    if success_count == 0:
        raise ValueError("etl_load_log has no success row for source_file")

    if success_count > 1:
        raise ValueError("etl_load_log has more than one success row for source_file")

    log_sql = f"""
        select rows_loaded, status, error_message
        from {LOG_TABLE}
        where flow_name = %s
          and source_file = %s
          and status = 'success'
        order by loaded_at desc
        limit 1;
    """

    rows_loaded, status, error_message = fetch_one(cursor, log_sql, (FLOW_NAME, source_file))

    logger.info("log rows_loaded: %s", rows_loaded)
    logger.info("log status: %s", status)
    logger.info("log error_message: %s", error_message)

    if rows_loaded != rows_cnt:
        raise ValueError("rows_loaded does not match flights_raw rows")

    if error_message is not None:
        raise ValueError("success log row has error_message")


def check_airport_links(cursor, source_file):
    origin_sql = f"""
        select count(*)
        from (
            select distinct origin_code as airport_code
            from {FLIGHTS_TABLE}
            where source_file = %s
              and origin_code is not null

            except

            select distinct iata_code as airport_code
            from {AIRPORTS_TABLE}
            where iata_code is not null
        ) as missing_origin_codes;
    """

    dest_sql = f"""
        select count(*)
        from (
            select distinct dest_code as airport_code
            from {FLIGHTS_TABLE}
            where source_file = %s
              and dest_code is not null

            except

            select distinct iata_code as airport_code
            from {AIRPORTS_TABLE}
            where iata_code is not null
        ) as missing_dest_codes;
    """

    missing_origin_cnt = fetch_one(cursor, origin_sql, (source_file,))[0]
    missing_dest_cnt = fetch_one(cursor, dest_sql, (source_file,))[0]

    logger.info("missing origin airport codes: %s", missing_origin_cnt)
    logger.info("missing dest airport codes: %s", missing_dest_cnt)

    if missing_origin_cnt > 0:
        logger.warning("some origin_code values have no match in airports.iata_code")

    if missing_dest_cnt > 0:
        logger.warning("some dest_code values have no match in airports.iata_code")


def run_checks(source_file):
    logger.info("run stg quality checks")

    connection = get_postgres_connection()

    try:
        with connection.cursor() as cursor:
            check_airports(cursor)
            rows_cnt = check_flights_raw(cursor, source_file)
            check_load_log(cursor, source_file, rows_cnt)
            check_airport_links(cursor, source_file)
    finally:
        connection.close()

    logger.info("stg quality checks finished")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--flight-date",
        required=True,
        help="technical folder date in yyyy-mm-dd format",
    )
    parser.add_argument(
        "--flights-prefix",
        default="flights_us_data",
        help="s3 prefix with flights files",
    )
    args = parser.parse_args()

    setup_logging()
    load_dotenv()

    check_flight_date(args.flight_date)

    source_file = build_source_key(args.flights_prefix, args.flight_date)
    run_checks(source_file)


if __name__ == "__main__":
    main()
