from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import BashOperator
from airflow.utils.dates import days_ago

###############################################
# Parameters
###############################################
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

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
        dag_id="Arrival_Time_Preprocess_4-49_1-59_3-1_daily", 
        description="Arrival Time Preprocess",
        default_args=default_args, 
        start_date = days_ago(1),
        schedule_interval='0 2 * * *',
        max_active_runs=1,
        catchup=False
    )


arrival_time = BashOperator(
    task_id = 'arrival_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_multiroute_daily_schedule.py --route_num_01 "4-49(170)" --route_num_02 "1-59(526)" --route_num_03 "3-1(2)"',
    dag = dag
)
# '4-49(170)', '1-59(526)', '3-1(2)'

link_time = BashOperator(
    task_id = 'link_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/link_time_postgres.py --route_num_01 "4-49(170)" --route_num_02 "1-59(526)" --route_num_03 "3-1(2)"',
    dag = dag
)

arrival_time >> link_time