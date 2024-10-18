from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# DAG 정의
with DAG(
    'sample_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='A simple tutorial DAG',
    schedule='@daily',  # schedule_interval 대신 schedule 사용
    start_date=datetime(2023, 10, 15),  # start_date 추가
    catchup=False,  # catchup=False로 설정
) as dag:
    # Task 정의
    start_task = EmptyOperator(
        task_id='start'
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> end_task
