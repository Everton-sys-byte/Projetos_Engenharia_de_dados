from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner' : 'Everton Soares',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

# função python q será chamada dentro da DAG no operator (python_callable)
def greet(name, age):
    print(f"Olá, Mundo! Meu nome é {name} e eu tenho {age} anos")

def getName():
    return 'Everton'

with DAG(
    dag_id = 'DAG_With_Python_Operator_Return_Data',
    description = 'Dag com operador Python',
    start_date = datetime(2024, 9, 20),
    schedule_interval = '@daily',
    default_args = default_args
) as dag :
    # op_kwargs está passando os valores via parametro para a função greet()
    # task1 = PythonOperator(task_id='ola_mundo', python_callable=greet, op_kwargs={'name': 'Everton', 'age' : 26})
    task2 = PythonOperator(task_id='get_nome', python_callable=getName)

    task2