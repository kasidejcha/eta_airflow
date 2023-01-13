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
        dag_id="import_daily_gps_data_thaistar",
        description="Arrival Time Preprocess",
        default_args=default_args,
        start_date = days_ago(1),
        schedule_interval='0 3 * * *',
        max_active_runs=1,
        catchup=False
    )


pg2csv = BashOperator(
    task_id = 'pg2csv',
    bash_command = f'python /usr/local/spark/app/pipeline_scripts/pg2csv_thaistar.py',
    dag = dag
)

# arrival_time = BashOperator(
#     task_id = 'arrival_time',
#     bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_multiroute_daily_schedule.py --route_num_01 "1-3(34)"',
#     dag = dag
# )
#  '1-3(34)'

pg2csv