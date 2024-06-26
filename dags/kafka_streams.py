from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import logging
from kafka import KafkaProducer
import time

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 6, 25, 10, 00)
}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} " \
                     f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting data streaming process")
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        logger.info("Kafka Producer initialized")
        curr_time = time.time()

        while True:
            if time.time() > curr_time + 60:  # Stream for 1 minute
                break
            try:
                res = get_data()
                formatted_data = format_data(res)
                producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
                logger.info("Message sent to Kafka topic 'users_created'")
            except Exception as e:
                logger.error(f'An error occurred: {e}')
                continue

    except Exception as e:
        logger.error(f"An error occurred in the streaming process: {e}")

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
