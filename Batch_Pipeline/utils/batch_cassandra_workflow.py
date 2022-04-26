from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    'owner': 'martin',
    'depends_on_past': False,
    'email': ['martinsheard96@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 4, 24), # update as per user requirements
    'retry_delay': timedelta(minutes=5), 
    'end_date': datetime(2023, 4, 28), # update as per user requirements
}

with DAG(dag_id='pinterest_workflow',
         default_args=default_args,
         schedule_interval='0 08/24 * * *', # change to once a day
         catchup=False,
         tags=['test']
         ) as dag:

    send_cassandra = BashOperator(
        task_id='cassandra',
        bash_command= 'cd ~/Documents/DATA_ENGINEERING/Project_2_Pinterest_Data_Pipeline/Batch_Pipeline && python3 main.py',
        dag=dag)
