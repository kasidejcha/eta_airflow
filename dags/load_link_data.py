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
        dag_id="load_link_data",
        description="Arrival Time Preprocess",
        default_args=default_args,
        start_date = days_ago(1),
        schedule_interval='@once',
        max_active_runs=1,
        catchup=False
    )

link_data = BashOperator(
    task_id = 'link_data',
    bash_command = 'python /usr/local/spark/app/MDVR/load_link_data.py',
    dag = dag
)

link_data