"""
Code to create the dag to download new data every 15min from the website
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('gdelt_dag', default_args=default_args, schedule_interval=timedelta(minutes=15))


# cd into directory where the script is
cd_into = BashOperator(
    task_id='cd_into',
    bash_command='cd /home/ubuntu/insight-gdelt/src/tools/',
    dag=dag)

# execute the script
upload_data_to_s3 = BashOperator(
    task_id='upload_data',
    bash_command='python download_source.py -t 2 -c 3',
    dag=dag)

# create dependencies
upload_data_to_s3.set_upstream(cd_into)
