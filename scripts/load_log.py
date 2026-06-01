LOG_TABLE = "team_vdga_metadata.etl_load_log"


def is_file_loaded(cursor, flow_name, source_key):
    sql = f"""
        select 1
        from {LOG_TABLE}
        where flow_name = %s
          and source_file = %s
          and status = 'success'
        limit 1
    """

    cursor.execute(sql, (flow_name, source_key))
    row = cursor.fetchone()

    return row is not None


def write_load_log(
    cursor,
    flow_name,
    source_key,
    flight_dt,
    rows_loaded,
    status,
    error_message=None,
):
    sql = f"""
        insert into {LOG_TABLE} (
            flow_name,
            source_file,
            flight_dt,
            rows_loaded,
            status,
            error_message,
            loaded_at,
            updated_at
        )
        values (%s, %s, %s, %s, %s, %s, now(), now())
    """

    cursor.execute(
        sql,
        (
            flow_name,
            source_key,
            flight_dt,
            rows_loaded,
            status,
            error_message,
        ),
    )
