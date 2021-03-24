# Defining a function that uses the python logger to log a function
# Trigger this function using an Airflow task. 
# Airflow can be used to trigger heavy processing steps for analytical DWH

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# define demo functions
# purpose: show how tasks can be coordinated
def say_hi():
    logging.info("Hi, there!")

def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 -2 = {6-2}")


def division():
    logging.info(f"10 / 2 = {int(10/2)}")


dag = DAG(
        'dag_exercise',
        # start_date: run now 
        # time_delta: backfill data for last 30 days 
        start_date=datetime.datetime.now() - datetime.timedelta(days=30),
        schedule_interval='@monthly')

# add tasks for Airflow to execute

# Parameters:
# task_id = name it anything as long as it is somewhat descriptive
# python_callable = passing function to PythonOperator so it knows which function to execute
# dag = assign 'dag' to the DAG field for the PythonOperator so it knows what DAG it belongs to


# Define an say hi task that calls the `say_hi` function above
say_hi_task = PythonOperator(
    task_id="say_hi",
    python_callable=say_hi,
    dag=dag
)

# Define an addition task that calls the `addition` function above
addition_task = PythonOperator(
    task_id="addition_task",
    python_callable=addition,
    dag=dag
)

# Define a subtraction task that calls the `subtraction` function above
subtraction_task = PythonOperator(
    task_id="subtraction_task",
    python_callable=subtraction,
    dag=dag
)

# Define a division task that calls the `division` function above
division_task = PythonOperator(
    task_id="division_task",
    python_callable=division,
    dag=dag
)

# Configuring task dependencies such that the graph looks like the following:
#
#                    -> addition_task
#                   /                  \
#        say_hi_task                    -> division_task
#                   \                  /
#                    ->subtraction_task

say_hi_task >> addition_task
say_hi_task >> subtraction_task
division_task << addition_task
division_task << subtraction_task