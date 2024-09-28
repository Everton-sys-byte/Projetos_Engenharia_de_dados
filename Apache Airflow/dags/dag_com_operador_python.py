from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner' : 'Everton Soares',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

# função python q será chamada dentro da DAG no operator (python_callable)
def greet():
    print("Olá, Mundo!")

with DAG(
    dag_id = 'DAG_With_Python_Operator',
    description = 'Dag com operador Python',
    start_date = datetime(2024, 9, 20),
    schedule_interval = '@daily',
    default_args = default_args
) as dag :
    task1 = PythonOperator(task_id='ola_mundo', python_callable=greet)
    
    task1