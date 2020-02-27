from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# import db_checker
from airflow.operators.dummy_operator import DummyOperator
import os


def print_date():
    print(datetime.today())
    return "Printed today's date"

default_args = {
    'owner': 'CG',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'simple_DAG',
    default_args=default_args,
    description='Simple_DAG',
    schedule_interval=timedelta(days=1)
)

t1 = DummyOperator(
    task_id='Create_the_DB_list',
    dag=dag
)

t2 = PythonOperator(
    task_id='Check_DB',
    python_callable = print_date,
    depends_on_past=False,
    dag=dag
)

t1 >> t2
