# Defining a function that uses the python logger to log a function
# Trigger this function using an Airflow task. 
# Airflow can be used to trigger heavy processing steps for analytical DWH

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# define a simple function
def say_hi():
    logging.info("Hi, there!")


dag = DAG(
        'exercise',
        start_date=datetime.datetime.now() - datetime.timedelta(days=30),
        schedule_interval='@monthly')

# add a greet task for Airflow to execute
greet_task = PythonOperator(
    task_id="greet_task", # id could by anything as long as it is somewhat descriptive
    python_callable=say_hi, # passing function to PythonOperator so it knows which function to execute
    dag=dag # assign 'dag' to the DAG field for the PythonOperator so it knows what DAG it belongs to
)
