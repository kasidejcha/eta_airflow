from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"
all_stations = f"/usr/local/spark/resources/data/all_stations.csv"

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
        dag_id="Junctions", 
        description="This DAG is a sample of integration between Spark and DB. It reads CSV files and load them into a Postgres DB",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )


spark_job_load_arrival_time = SparkSubmitOperator(
    task_id="spark_job_load_arrival_time",
    application="/usr/local/spark/app/find_dis_spark.py", # Spark application path created in airflow and spark cluster
    name="arrival-time-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[all_stations],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

spark_job_load_arrival_time >> end