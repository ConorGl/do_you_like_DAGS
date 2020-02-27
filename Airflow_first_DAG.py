from datetime import  datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago We will use this at another time but today is perfect
import db_checker
from airflow.operators.bash_operator import BashOperator
import os

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
    'first_DAG',
    default_args=default_args,
    description='My_first_DAG_with_Airflow',
    schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='Create_the_DB_list',
    bash_command='python3 '+os.environ['PYTHONPROJECT']+'json_converter.py',
    dag=dag
)

t2 = PythonOperator(
    task_id='Check_DB',
    python_callable = db_checker.print_results,
    depends_on_past=True,
    dag=dag
)

t1 >> t2
