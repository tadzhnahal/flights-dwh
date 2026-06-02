import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DAG_ID = "team_vdga_stg_dag"
POSTGRES_CONN_ID = "edu_dwh_postgres"
S3_CONN_ID = "team_vdga_s3"
DEFAULT_FLIGHT_DATE = "2026-03-01"

PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
SCRIPTS_DIR = PROJECT_DIR / "scripts"


DEFAULT_ARGS = {
    "owner": "team_vdga",
    "depends_on_past": False,
}


def get_postgres_connection():
    import psycopg2

    airflow_connection = BaseHook.get_connection(POSTGRES_CONN_ID)

    return psycopg2.connect(
        host=airflow_connection.host,
        port=airflow_connection.port or 5432,
        dbname=airflow_connection.schema,
        user=airflow_connection.login,
        password=airflow_connection.password,
        connect_timeout=5,
    )


def add_postgres_env(script_env):
    airflow_connection = BaseHook.get_connection(POSTGRES_CONN_ID)

    script_env["POSTGRES_HOST"] = airflow_connection.host
    script_env["POSTGRES_PORT"] = str(airflow_connection.port or 5432)
    script_env["POSTGRES_DB"] = airflow_connection.schema
    script_env["POSTGRES_USER"] = airflow_connection.login
    script_env["POSTGRES_PASSWORD"] = airflow_connection.password


def add_s3_env(script_env):
    airflow_connection = BaseHook.get_connection(S3_CONN_ID)

    script_env["AWS_ACCESS_KEY_ID"] = airflow_connection.login
    script_env["AWS_SECRET_ACCESS_KEY"] = airflow_connection.password


def get_script_env():
    script_env = os.environ.copy()

    add_postgres_env(script_env)
    add_s3_env(script_env)

    script_env["S3_ENDPOINT_URL"] = script_env.get(
        "S3_ENDPOINT_URL",
        "https://storage.yandexcloud.net",
    )
    script_env["AWS_DEFAULT_REGION"] = script_env.get(
        "AWS_DEFAULT_REGION",
        "ru-central1",
    )
    script_env["SOURCE_S3_BUCKET"] = script_env.get(
        "SOURCE_S3_BUCKET",
        "gsbdwhdata",
    )
    script_env["FLIGHTS_PREFIX"] = script_env.get(
        "FLIGHTS_PREFIX",
        "flights_us_data",
    )
    script_env["AIRPORTS_CSV_URL"] = script_env.get(
        "AIRPORTS_CSV_URL",
        "https://ourairports.com/data/airports.csv",
    )

    old_pythonpath = script_env.get("PYTHONPATH", "")

    if old_pythonpath:
        script_env["PYTHONPATH"] = f"{SCRIPTS_DIR}:{old_pythonpath}"
    else:
        script_env["PYTHONPATH"] = str(SCRIPTS_DIR)

    return script_env


def get_flight_date(**context):
    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf:
        flight_date = dag_run.conf.get("flight_date")

        if flight_date:
            return flight_date

    return DEFAULT_FLIGHT_DATE


def run_sql_file(sql_file_name):
    sql_path = SQL_DIR / sql_file_name

    if not sql_path.exists():
        raise FileNotFoundError(f"sql file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")

    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text)
    finally:
        connection.close()


def run_project_script(script_file_name, script_args, **context):
    script_path = SCRIPTS_DIR / script_file_name

    if not script_path.exists():
        raise FileNotFoundError(f"script file not found: {script_path}")

    flight_date = get_flight_date(**context)

    rendered_args = []

    for script_arg in script_args:
        if script_arg == "{{ flight_date }}":
            rendered_args.append(flight_date)
        else:
            rendered_args.append(script_arg)

    command = [
        sys.executable,
        str(script_path),
    ] + rendered_args

    print("run command:", " ".join(command))
    print("project dir:", PROJECT_DIR)
    print("scripts dir:", SCRIPTS_DIR)
    print("flight date:", flight_date)

    result = subprocess.run(
        command,
        cwd=str(PROJECT_DIR),
        env=get_script_env(),
        text=True,
        capture_output=True,
    )

    if result.stdout:
        print("script stdout:")
        print(result.stdout)

    if result.stderr:
        print("script stderr:")
        print(result.stderr)

    result.check_returncode()


with DAG(
    dag_id=DAG_ID,
    description="STG pipeline for team_vdga flights project",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["team_vdga", "stg", "flights"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    create_schemas = PythonOperator(
        task_id="create_schemas",
        python_callable=run_sql_file,
        op_kwargs={
            "sql_file_name": "01_create_schemas.sql",
        },
    )

    create_load_log_table = PythonOperator(
        task_id="create_load_log_table",
        python_callable=run_sql_file,
        op_kwargs={
            "sql_file_name": "02_create_load_log.sql",
        },
    )

    create_airports_table = PythonOperator(
        task_id="create_airports_table",
        python_callable=run_sql_file,
        op_kwargs={
            "sql_file_name": "03_create_stg_airports.sql",
        },
    )

    create_flights_raw_table = PythonOperator(
        task_id="create_flights_raw_table",
        python_callable=run_sql_file,
        op_kwargs={
            "sql_file_name": "04_create_stg_flights_raw.sql",
        },
    )

    load_airports = PythonOperator(
        task_id="load_airports",
        python_callable=run_project_script,
        op_kwargs={
            "script_file_name": "load_airports.py",
            "script_args": [],
        },
    )

    load_flights_raw = PythonOperator(
        task_id="load_flights_raw",
        python_callable=run_project_script,
        op_kwargs={
            "script_file_name": "load_flights_raw.py",
            "script_args": [
                "--flight-date",
                "{{ flight_date }}",
                "--load-to-postgres",
            ],
        },
    )

    check_stg_quality = PythonOperator(
        task_id="check_stg_quality",
        python_callable=run_project_script,
        op_kwargs={
            "script_file_name": "check_stg_quality.py",
            "script_args": [
                "--flight-date",
                "{{ flight_date }}",
            ],
        },
    )

    finish = EmptyOperator(
        task_id="finish",
    )

    (
        start
        >> create_schemas
        >> create_load_log_table
        >> create_airports_table
        >> create_flights_raw_table
        >> load_airports
        >> load_flights_raw
        >> check_stg_quality
        >> finish
    )
