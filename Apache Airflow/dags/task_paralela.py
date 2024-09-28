from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# configuração da DAG
default_args = {
    'owner' : 'Everton Soares',
    'retries' : 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'task_paralela', default_args = default_args, description = 'Minha primeira dag mas na versão dois', 
    start_date=datetime(2024, 9, 15), schedule_interval='@daily') as dag:

    task1 = BashOperator(task_id='primeira_task', bash_command="echo 'Hello World'")

    # segunda task q vai ser executada depois do sucesso da task 1
    task2 = BashOperator(task_id='segunda_task', bash_command= "echo 'Sou a segunda task, serei executada depois do sucesso da tarefa 1'")

    # terceira task q vai ser executada depois do sucesso da task 3
    task3 = BashOperator(task_id='terceira_task', bash_command= "echo 'Olá, sou a terceira tarefa, serei executada depois do sucesso da tarefa 1'")

    task1 >> [task2, task3]