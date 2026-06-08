import os
import subprocess
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

DAG_ID = "team_vdga_dm_dag"
POSTGRES_CONN_ID = "edu_dwh_postgres"

PROJECT_DIR = Path(__file__).resolve().parents[1]


def add_postgres_env(script_env):
    airflow_connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    script_env["POSTGRES_HOST"] = airflow_connection.host
    script_env["POSTGRES_PORT"] = str(airflow_connection.port or 5432)
    script_env["POSTGRES_DB"] = airflow_connection.schema
    script_env["POSTGRES_USER"] = airflow_connection.login
    script_env["POSTGRES_PASSWORD"] = airflow_connection.password


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
    description="DM pipeline for team_vdga flights project",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["team_vdga", "dm", "flights"],
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_run_dm = PythonOperator(
        task_id="dbt_run_dm",
        python_callable=run_dbt_command,
        op_kwargs={
            "args": ["run", "--no-partial-parse", "--select", "path:models/darandgoncharova"]
        },
    )

    dbt_test_dm = PythonOperator(
        task_id="dbt_test_dm",
        python_callable=run_dbt_command,
        op_kwargs={
            "args": ["test", "--no-partial-parse", "--select", "path:models/darandgoncharova"]
        },
    )

    finish = EmptyOperator(task_id="finish")

    start >> dbt_run_dm >> dbt_test_dm >> finish