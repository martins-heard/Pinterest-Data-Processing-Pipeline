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
    'start_date': datetime(2022, 3, 18),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 3, 25),
}

with DAG(dag_id='pinterest_workflow',
         default_args=default_args,
         schedule_interval='*/1 * * * *', # change to once a day
         catchup=False,
         tags=['test']
         ) as dag:
    
    # batch_consumer = BashOperator( # Task 4: run batch_consumer.py
    #     task_id='batch_consumer',
    #     bash_command= 'cd ~/Documents/DATA_ENGINEERING/Project_2_Pinterest_Data_Pipeline/Pinterest_App/API && python3 batch_consumer.py',
    #     dag=dag)

    send_hbase = BashOperator(
        task_id='hbase',
        bash_command= 'cd ~/Documents/DATA_ENGINEERING/Project_2_Pinterest_Data_Pipeline/Pinterest_App && python3 S3_Spark_Hbase.py',
        dag=dag)
