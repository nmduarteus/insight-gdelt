"""
Code to create the dag to download new data every 15min from the website
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('gdelt_plus_dag', default_args=default_args, schedule_interval=timedelta(minutes=15))

# execute the script
upload_data_to_s3 = BashOperator(
    task_id='upload_data',
    bash_command='python3 /home/ubuntu/insight-gdelt/src/tools/download_source.py -t 2 -c 3',
    dag=dag)
