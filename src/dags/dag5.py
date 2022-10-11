import requests
import json
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine

DAG_ID = "task_2_ranks_stg0123"

postgres_hook_dwh_system = PostgresHook("PG_WAREHOUSE_CONNECTION")
engine_dwh_system = postgres_hook_dwh_system.get_sqlalchemy_engine()

headers={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f", # ключ API
    "X-Nickname": "tolmachevs", # авторизационные данные
    "X-Cohort": "0" # авторизационные данные
}

def restaurants_to_stg():
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    method_url = '/restaurants'
    payload = {'limit': '1'}
    r = requests.get(url + method_url, headers=headers)
    response_dict = json.loads(r.content)
    df = pd.DataFrame(response_dict)
    df.to_sql(name='restaurants', schema='stg', con=engine_dwh_system, if_exists='replace', index=False)
    
def couriers_to_stg():
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    method_url = '/couriers'
    payload = {'limit': '1'}
    r = requests.get(url + method_url, headers=headers)
    response_dict = json.loads(r.content)
    df = pd.DataFrame(response_dict)
    df.to_sql(name='couriers', schema='stg', con=engine_dwh_system, if_exists='replace', index=False)
    
def deliveries_to_stg():
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    method_url = '/deliveries'
    payload = {'limit': '1'}
    r = requests.get(url + method_url, headers=headers)
    response_dict = json.loads(r.content)
    df = pd.DataFrame(response_dict)
    df.to_sql(name='deliveries', schema='stg', con=engine_dwh_system, if_exists='replace', index=False)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2022, 2, 2),
    #schedule_interval=timedelta(minutes=15),
    #catchup=False,
) as dag:
    
    into_restaurants_to_stg = PythonOperator(task_id='restaurants', python_callable=restaurants_to_stg)
    into_couriers_to_stg = PythonOperator(task_id='couriers', python_callable=couriers_to_stg)
    into_deliveries_to_stg = PythonOperator(task_id='deliveries', python_callable=deliveries_to_stg)

into_restaurants_to_stg >> into_couriers_to_stg >> into_deliveries_to_stg
