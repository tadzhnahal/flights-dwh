import os
import subprocess
import sys
import io
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DAG_ID = "team_vdga_dds_dag"
POSTGRES_CONN_ID = "edu_dwh_postgres"

PROJECT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_DIR / "scripts"
SQL_DIR = PROJECT_DIR / "sql"


DEFAULT_ARGS = {
    "owner": "team_vdga",
    "depends_on_past": False,
}


def add_postgres_env(script_env):
    airflow_connection = BaseHook.get_connection(POSTGRES_CONN_ID)

    script_env["POSTGRES_HOST"] = airflow_connection.host
    script_env["POSTGRES_PORT"] = str(airflow_connection.port or 5432)
    script_env["POSTGRES_DB"] = airflow_connection.schema
    script_env["POSTGRES_USER"] = airflow_connection.login
    script_env["POSTGRES_PASSWORD"] = airflow_connection.password

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


def get_script_env():
    script_env = os.environ.copy()

    add_postgres_env(script_env)

    old_pythonpath = script_env.get("PYTHONPATH", "")

    if old_pythonpath:
        script_env["PYTHONPATH"] = f"{SCRIPTS_DIR}:{old_pythonpath}"
    else:
        script_env["PYTHONPATH"] = str(SCRIPTS_DIR)

    return script_env

def load_airports_timezones():
    file_path = PROJECT_DIR / "data" / "timezones.csv"

    df = pd.read_csv(file_path)

    conn = get_postgres_connection()

    try:
        with conn:
            with conn.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS team_vdga_dds.airports_timezones (
                        iata_code TEXT,
                        timezone TEXT
                    )
                """)

                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cur.copy_expert("""
                    COPY team_vdga_dds.airports_timezones (iata_code, timezone)
                    FROM STDIN WITH CSV
                """, buffer)

    finally:
        conn.close()

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


def run_dbt_command(args):
    cmd = ["dbt"] + args

    env = os.environ.copy()
    add_postgres_env(env)
    env["DBT_PROFILES_DIR"] = str(PROJECT_DIR)

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_DIR / "dbt"),
        env=env,
        text=True,
        capture_output=True,
    )

    print("DBT target db =", env.get("POSTGRES_DB"))
    print("DBT host =", env.get("POSTGRES_HOST"))

    print(result.stdout)
    print(result.stderr)

    result.check_returncode()

with DAG(
    dag_id=DAG_ID,
    description="DDS pipeline for team_vdga flights project",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["team_vdga", "dds", "flights"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_dds_schema = PythonOperator(
    task_id="create_dds_schema",
    python_callable=run_sql_file,
    op_kwargs={
        "sql_file_name": "create_schema_dds.sql",
    },
    )
    """
    dbt_deps = PythonOperator(
    task_id="dbt_deps",
    python_callable=run_dbt_command,
    op_kwargs={
        "args": ["deps"]
    },
    )
    """
    load_airports_timezones = PythonOperator(
    task_id="load_airports_timezones",
    python_callable=load_airports_timezones,
    )

    dbt_run = PythonOperator(
    task_id="dbt_run_dds",
    python_callable=run_dbt_command,
    op_kwargs={
        "args": ["run", "--select", "flights_performed", "flights_cancelled"]
    },
    )

    dbt_test = PythonOperator(
    task_id="dbt_test_dds",
    python_callable=run_dbt_command,
    op_kwargs={
        "args": ["test", "--select", "flights_performed", "flights_cancelled"]
    },
    )

    finish = EmptyOperator(task_id="finish")
    # >> dbt_deps was removed 
    start >> create_dds_schema >> load_airports_timezones >> dbt_run >> dbt_test >> finish 