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
now = datetime.now()

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
        dag_id="import_daily_gps_data", 
        description="Arrival Time Preprocess",
        default_args=default_args, 
        start_date = days_ago(1),
        schedule_interval='15 17 * * *',
        max_active_runs=1,
        catchup=False
    )

mongo2pg = BashOperator(
    task_id = 'mongo2pg',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/mongo2pg.py',
    dag = dag
)

pg2csv = BashOperator(
    task_id = 'pg2csv',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/pg2csv.py',
    dag = dag
)

# arrival_time = BashOperator(
#     task_id = 'arrival_time',
#     bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_multiroute_daily_schedule.py --route_num_01 "1-3(34)"',
#     dag = dag
# )
#  '1-3(34)'

mongo2pg >> pg2csv