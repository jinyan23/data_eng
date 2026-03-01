# import the libraries

from datetime import timedelta, datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# defining DAG arguments
default_args = {
    'owner': 'jyzhang',
    'start_date': datetime(2025, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='transport_dag',
    default_args=default_args,
    description='Transport DAG'
)

# define the first task
extract = BashOperator(
    task_id='echo',
    bash_command='echo "hello world"',
    dag=dag,
)

extract