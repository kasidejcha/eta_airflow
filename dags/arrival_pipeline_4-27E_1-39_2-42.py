from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import BashOperator

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
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="Arrival_Time_Preprocess_4-27E_1-39_2-42", 
        description="Arrival Time Preprocess",
        default_args=default_args, 
        schedule_interval='@once'
    )



arrival_time = BashOperator(
    task_id = 'arrival_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_multiroute_batch_m12.py --route_num_01 "4-27E(173)" --route_num_02 "1-39(71)" --route_num_03 "2-42(44)"',
    dag = dag
)
# '4-27E(173)', '1-39(71)', '2-42(44)'

arrival_time