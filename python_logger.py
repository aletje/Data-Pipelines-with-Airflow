# Defining a function that uses the python logger to log a function

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def say_hi():
    logging.info("Hi, there!")


dag = DAG(
        'lesson1.exercise1',
        start_date=datetime.datetime.now())


greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=say_hi,
    dag=dag
)
