from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Producer

def produce_message():
    kafka_broker = '10.107.135.48:9092'  # Kafka 브로커 IP
    producer = Producer({'bootstrap.servers': kafka_broker})

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # 메시지를 보냅니다
    producer.produce('my-topic', key='key', value='Hello, Kafka! It is from Airflow!', callback=delivery_report)
    
    # 모든 메시지를 전송하고 콜백을 기다림
    producer.flush(timeout=10)  # 10초 타임아웃 설정

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 15),
    'retries': 1,
}

with DAG('kafka_test_dag', default_args=default_args, schedule='* * * * *', catchup=False) as dag:
    produce_task = PythonOperator(
        task_id='produce_kafka_message',
        python_callable=produce_message,
    )

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from confluent_kafka import Producer, Consumer
# from datetime import datetime

# # Kafka 브로커와 토픽 설정
# KAFKA_BROKER = '10.104.219.151:9092'  # 직접 IP로 변경  # 또는 9091, 9093 사용 가능
# TOPIC = 'my-topic'

# def produce_message(**kwargs):
#     """Kafka에 메시지를 프로듀스하는 함수"""
#     producer = Producer({'bootstrap.servers': KAFKA_BROKER})
#     message = 'Hello from Airflow!'
#     producer.produce(TOPIC, value=message)
#     producer.flush()
#     print(f'Produced message: {message}')

# def consume_message(**kwargs):
#     """Kafka에서 메시지를 소비하는 함수"""
#     consumer = Consumer({
#         'bootstrap.servers': KAFKA_BROKER,
#         'group.id': 'my-group',
#         'auto.offset.reset': 'earliest'
#     })
#     consumer.subscribe([TOPIC])
    
#     msg = consumer.poll(1.0)  # 1초 대기

#     if msg is None:
#         print("No message received")
#     elif msg.error():
#         print(f"Consumer error: {msg.error()}")
#     else:
#         print(f'Consumed message: {msg.value().decode("utf-8")}')

#     consumer.close()

# # DAG 정의
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 10, 15),
# }

# with DAG('kafka_dag', default_args=default_args, schedule='* * * * *', catchup=False) as dag:
#     producer_task = PythonOperator(
#         task_id='produce_message',
#         python_callable=produce_message,
#     )

#     consumer_task = PythonOperator(
#         task_id='consume_message',
#         python_callable=consume_message,
#     )

#     producer_task >> consumer_task  # Producer가 먼저 실행된 후 Consumer 실행
