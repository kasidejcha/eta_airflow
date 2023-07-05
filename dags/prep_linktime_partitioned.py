from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import BashOperator
from airflow.utils.dates import days_ago

###############################################
# Parameters
###############################################


###############################################
# DAG Definition
###############################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="prep_linktime_partitioned",
        description="Link time preprocess with partition",
        default_args=default_args,
        start_date = days_ago(1),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=1,
        catchup=False
    )

prep_linktime_partitioned = BashOperator(
    task_id = 'prep_linktime_partitioned',
    bash_command = 'python /usr/local/spark/app/MDVR/prep_linktime_partitioned.py',
    dag = dag
)

prep_linktime_partitioned