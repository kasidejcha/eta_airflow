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
        dag_id="Arrival_Time_Preprocess_thaistar_2-48_4-38_4-36_daily", 
        description="Arrival Time Preprocess",
        default_args=default_args, 
        schedule_interval='0 4 * * *'
    )



arrival_time = BashOperator(
    task_id = 'arrival_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/arrival_pipeline_thaistar_daily.py --route_num_01 "2-48(122)" --route_num_02 "4-38(28)" --route_num_03 "4-36(7)"',
    dag = dag
)


link_time = BashOperator(
    task_id = 'link_time',
    bash_command = 'python /usr/local/spark/app/pipeline_scripts/link_time_postgres.py --route_num_01 "2-48(122)" --route_num_02 "4-38(28)" --route_num_03 "4-36(7)"',
    dag = dag
)

arrival_time >> link_time