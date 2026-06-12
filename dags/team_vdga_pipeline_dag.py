from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DAG_ID = "team_vdga_pipeline_dag"

DEFAULT_FLIGHT_DATE = "2026-06-11"

FLIGHT_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('flight_date', '" + DEFAULT_FLIGHT_DATE + "') "
    "if dag_run and dag_run.conf else '" + DEFAULT_FLIGHT_DATE + "' }}"
)

DEFAULT_ARGS = {
    "owner": "team_vdga",
    "depends_on_past": False,
}


with DAG(
    dag_id=DAG_ID,
    description="Full team_vdga pipeline: STG -> DDS -> DM",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["team_vdga", "orchestrator", "flights"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    run_stg = TriggerDagRunOperator(
        task_id="run_stg",
        trigger_dag_id="team_vdga_stg_dag",
        trigger_run_id="pipeline_stg_{{ ts_nodash }}",
        conf={
            "flight_date": FLIGHT_DATE_TEMPLATE,
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    run_dds = TriggerDagRunOperator(
        task_id="run_dds",
        trigger_dag_id="team_vdga_dds_dag",
        trigger_run_id="pipeline_dds_{{ ts_nodash }}",
        conf={
            "flight_date": FLIGHT_DATE_TEMPLATE,
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    run_dm = TriggerDagRunOperator(
        task_id="run_dm",
        trigger_dag_id="team_vdga_dm_dag",
        trigger_run_id="pipeline_dm_{{ ts_nodash }}",
        conf={
            "flight_date": FLIGHT_DATE_TEMPLATE,
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    finish = EmptyOperator(
        task_id="finish",
    )

    start >> run_stg >> run_dds >> run_dm >> finish
