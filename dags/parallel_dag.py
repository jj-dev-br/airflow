from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag
from airflow.utils.task_group import TaskGroup

default_args = {
    'start_date': datetime(2020, 5, 5)
}

with DAG(
    dag_id =  'parallel_dag',
    schedule_interval= '@daily',
    default_args= default_args,
    catchup=False
    ) as dag:

    with TaskGroup('processing_tasks') as processing_tasks:
        task2 = BashOperator(
            task_id='task2',
            bash_command='sleep 3'
        )
        task3 = BashOperator(
            task_id='task3',
            bash_command='sleep 3'
        )

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 3'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='sleep 3'
    )

    task1>>processing_tasks>>task4