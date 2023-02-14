from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


now = datetime.now()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 15),
}

dag = DAG('Trigger_Parent_dag', max_active_runs=1, default_args=default_args, schedule_interval='@daily')

leave_work = DummyOperator(
    task_id='leave_work',
    dag=dag,
)
cook_dinner = DummyOperator(
    task_id='cook_dinner',
    dag=dag,
)

leave_work >> cook_dinner