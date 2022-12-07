from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
m = '08'
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"
arrival_files = f"/usr/local/spark/resources/data/arrival/m{m}/arrival_time/*.csv"
concat_arrival_file = f"/usr/local/spark/resources/data/arrival/m{m}/concat_arrival_time/arrival_time.csv"
link_folder = f"/usr/local/spark/resources/data/arrival/m{m}/links/"

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
        dag_id="spark_eta_upload_postgres", 
        description="This DAG is a sample of integration between Spark and DB. It reads CSV files and load them into a Postgres DB",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job_load_arrival_time = SparkSubmitOperator(
    task_id="spark_job_load_arrival_time",
    application="/usr/local/spark/app/arrival_time_postgres.py", # Spark application path created in airflow and spark cluster
    name="arrival-time-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[postgres_db,postgres_user,postgres_pwd, arrival_files, concat_arrival_file],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

spark_job_load_link_time = SparkSubmitOperator(
    task_id="spark_job_load_link_time",
    application="/usr/local/spark/app/link_time_postgres.py", # Spark application path created in airflow and spark cluster
    name="arrival-time-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[postgres_db,postgres_user,postgres_pwd, concat_arrival_file, link_folder],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_arrival_time >> spark_job_load_link_time >> end