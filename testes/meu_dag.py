from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta, timezone
data = datetime.today().strftime('%d%m%Y_%H_%M_%S')

with DAG(
    'meu_primeiro_dag',
    start_date=days_ago(2),
    schedule_interval='@daily'
    ) as dag:    

    tarefa_1 = DummyOperator(task_id = 'tarefa_1')    
    tarefa_2 = DummyOperator(task_id = 'tarefa_2')    
    tarefa_3 = DummyOperator(task_id = 'tarefa_3')
    tarefa_4 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = f'mkdir -p "/opt/airflow/dags/pasta_{data}"'
    )

    tarefa_1 >> [tarefa_2, tarefa_3]
    tarefa_3 >> tarefa_4  

