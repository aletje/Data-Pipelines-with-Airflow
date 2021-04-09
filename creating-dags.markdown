# Data Pipelines with Airflow

## Creating a DAG
(Notes from Udacity Data Engineering Nano Degree - lesson 1 chapter 13)

### Creating a DAG: 
- Give it a name
- description 
- start date
- interval

`from airflow import DAG`

{{% highlight python %}}
divvy_dag = DAG(
    'divvy',
    description='Analyzes Divvy Bikeshare Data',
    start_date=datetime(2019, 2, 4),
    schedule_interval='@daily')
Creating Operators to Perform Tasks
Operators define the atomic steps of work that make up a DAG. Instantiated operators are referred to as Tasks.

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print(“Hello World”)

divvy_dag = DAG(...)
task = PythonOperator(
    task_id=’hello_world’,
    python_callable=hello_world,
    dag=divvy_dag)

{{% endhighlight %}}

### Schedules
Schedules are optional, and may be defined with cron strings or Airflow Presets. Airflow provides the following presets:

`@once` - Run a DAG once and then never again
`@hourly` - Run the DAG every hour
`@daily` - Run the DAG every day
`@weekly` - Run the DAG every week
`@monthly` - Run the DAG every month
`@yearly`- Run the DAG every year
`None` - Only run the DAG when the user initiates it

### Start Date:
If your start date is in the past, Airflow will run your DAG as many times as there are schedule intervals between that start date and the current date.

### End Date: 
Unless you specify an optional end date, Airflow will continue to run your DAGs until you disable or delete the DAG.