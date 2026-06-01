from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


DAG_ID = "team_vdga_stg_dag"


DEFAULT_ARGS = {
    "owner": "team_vdga",
    "depends_on_past": False,
}


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

    finish = EmptyOperator(
        task_id="finish",
    )

    start >> finish
