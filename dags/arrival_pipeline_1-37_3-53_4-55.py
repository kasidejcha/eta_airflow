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
        dag_id="Arrival_Time_Preprocess_1-37_3-53_4-55", 
        description="Arrival Time Preprocess",
        default_args=default_args, 
        schedule_interval='@once'
    )



arrival_time = BashOperator(
    task_id = 'arrival_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_multiroute_batch_m12.py --route_num_01 "1-37(27)" --route_num_02 "3-53" --route_num_03 "4-55(163)"',
    dag = dag
)


arrival_time