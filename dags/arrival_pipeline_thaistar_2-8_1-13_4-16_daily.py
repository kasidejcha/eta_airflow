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
        dag_id="Arrival_Time_Preprocess_thaistar_2-8_1-13_4-16_daily",
        description="Arrival Time Preprocess",
        start_date = days_ago(1),
        default_args=default_args, 
        schedule_interval='0 4 * * *'
    )


arrival_time = BashOperator(
    task_id = 'arrival_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_thaistar_daily.py --route_num_01 "2-8(51)" --route_num_02 "1-13(126)" --route_num_03 "4-16(85)"',
    dag = dag
)

link_time = BashOperator(
    task_id = 'link_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/link_time_postgres.py --route_num_01 "2-8(51)" --route_num_02 "1-13(126)" --route_num_03 "4-16(85)"',
    dag = dag
)

arrival_time >> link_time