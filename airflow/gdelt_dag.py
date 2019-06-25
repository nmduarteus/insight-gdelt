"""
Code to create the dag to download new data from the website and process it
"""
from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

from airflow import DAG

# folder where the download script and the data folder are
SCRIPTS_FOLDER = '~/insight/scripts'
DATA_FOLDER = '~/insight/data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('gdelt_plus_dag', default_args=default_args, schedule_interval=timedelta(minutes=15))

# script command to download the data
download_bash = 'python3 ' + SCRIPTS_FOLDER + '/download_source.py -t 2 -c 3'
upload_data_to_s3 = BashOperator(
    task_id='upload_data',
    bash_command=download_bash,
    dag=dag)

# copy the latest data to spark master so that spark knows which date to run
copy_spark_date = BashOperator(
    task_id='copy_spark_date',
    bash_command='scp ' + DATA_FOLDER + '/spark.txt spark_master:/home/ubuntu/PycharmProjects/insight-gdelt/src',
    dag=dag)

# spark command to run in the spark master
spark_bash = "/usr/local/spark/bin/spark-submit /home/ubuntu/PycharmProjects/insight-gdelt/src/gdelt.py"

spark_job = SSHOperator(
    ssh_conn_id='ssh_default',
    task_id='spark_etl',
    command=spark_bash,
    dag=dag)

# dependencies
copy_spark_date.set_upstream(upload_data_to_s3)
spark_job.set_upstream(copy_spark_date)
