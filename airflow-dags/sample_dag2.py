from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# DAG 정의
with DAG(
    'sample_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='A simple tutorial DAG',
    schedule='@daily',  # 스케줄 설정
    start_date=datetime(2024, 10, 15),  # 시작 날짜 설정
    catchup=False,  # catchup 설정
) as dag:
    # Task 정의
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    start_task >> end_task
