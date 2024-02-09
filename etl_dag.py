'''
Airflow version <= 1.10 use airflow.contrib.sensors.file_sensor and airflow.operators.spark_submit_operator.
version > 1.10 use airflow.contrib.sensors.file_sensor airflow.contrib.operators.spark_submit_operator.
'''

from datetime import timedelta
import airflow
from airflow import DAG
#from airflow.operators.sensors import FileSensor
from airflow.contrib.sensors.file_sensor import FileSensor
#from airflow.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

import os
from datetime import date
from dataprocess.configloader import ConfigLoader

# Get base directory path
base_path = os.path.dirname(__file__)

# Get config file
config_file = os.path.join(base_path, 'config/config.ini')

# Create a ConfigParser object
config_loader = ConfigLoader(config_file)

# Access configuration values
file_path = config_loader.get_config_value('PATH', 'file_path')

# Get current date
today = date.today()
formatted_date = today.strftime('%d%m%Y')

# Get event data file path
input_path = os.path.join(base_path, file_path)


default_args = {
    'owner': 'airflow',    
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag_etl = DAG(
    dag_id = "dag_truecaller_etl",
    catchup=True,
    default_args=default_args,
    schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='Truecaller daily ETL job',
    start_date = airflow.utils.dates.days_ago(1) 
)

# Check event data availability
file_availability = FileSensor(
    task_id = "file_availability",
    filepath="{}{}.csv".format(input_path, formatted_date),
    poke_interval=1000,
    mode='poke',
    dag = dag_etl
    )

# Spark data transformation tasks
transformation = SparkSubmitOperator(
    task_id='spark_transformation',
    application='transformer.py',
    conn_id='spark_conn',
    verbose=False,
    task_concurrency=1,
    dag = dag_etl
    )


end_task = DummyOperator(task_id='end_task', dag=dag_etl)

file_availability >> transformation >> end_task


if __name__ == "__main__":
    dag_etl.cli()