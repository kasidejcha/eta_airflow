from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"

destination_file = "/usr/local/spark/resources/data/spark_files"
mssql_driver_jar = "/usr/local/spark/resources/jars/mssql-jdbc-11.2.0.jre8.jar"

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
        dag_id="spark-pull-mssql", 
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_pull_mssql = SparkSubmitOperator(
    task_id="spark_pull_mssql",
    application="/usr/local/spark/app/mssql_pull.py", # Spark application path created in airflow and spark cluster
    name="mssql_pulls",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[destination_file],
    jars=mssql_driver_jar,
    driver_class_path=mssql_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_pull_mssql >> end