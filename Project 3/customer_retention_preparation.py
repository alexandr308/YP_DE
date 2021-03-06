import time
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host
'''
http_conn_id
host: 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'
'''

postgres_conn_id = 'postgresql_de'
postgres_hook = PostgresHook(postgres_conn_id)
engine = postgres_hook.get_sqlalchemy_engine()
conn = engine.connect()

'''
postgresql_de
host: localhost
schema: de
port: 5432
login: jovyan
password: jovyan
'''

nickname = 'alexandr308'
cohort = '1'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


#скрипты для создания таблиц
user_order_log = '''
DROP TABLE IF EXISTS staging.user_order_log;

CREATE TABLE staging.user_order_log(
   id SERIAL,
   date_time TIMESTAMP,
   city_id INTEGER,
   city_name VARCHAR(100),
   customer_id BIGINT,
   first_name VARCHAR(100),
   last_name VARCHAR(100),
   item_id INTEGER,
   item_name VARCHAR(100),
   quantity BIGINT,
   payment_amount numeric(14,2),
   PRIMARY KEY (id)
);
CREATE INDEX main2 ON staging.user_order_log (customer_id);
'''

customer_research = '''
DROP TABLE IF EXISTS staging.customer_research;

CREATE TABLE staging.customer_research(
   id SERIAL,
   date_id TIMESTAMP,
   category_id INTEGER,
   geo_id INTEGER,
   sales_qty INTEGER,
   sales_amt NUMERIC(14,2),
   PRIMARY KEY (id)
);
CREATE INDEX main3 ON staging.customer_research (category_id);
'''

user_activity_log = '''
DROP TABLE IF EXISTS staging.user_activity_log;

CREATE TABLE staging.user_activity_log(
   id SERIAL,
   date_time TIMESTAMP,
   action_id BIGINT,
   customer_id BIGINT,
   quantity BIGINT,
   PRIMARY KEY (id)
);

CREATE INDEX main1 ON staging.user_activity_log (customer_id);
'''


def create_tables(query):
    conn.execute(query)


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    report_id = ti.xcom_pull(key='report_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{report_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    if 'id' in df.columns:
        df.drop_duplicates(subset=['id'])
        df.drop('id', axis=1, inplace=True)
    if filename in ['user_orders_log.csv']:
        if 'status' in df.columns:
            df['status'].fillna(value='shipped', inplace=True)
            df['payment_amount'] = np.where(df['status'] == 'refunded', (df['payment_amount'] * (-1)), df['payment_amount'])
            df.drop('status', axis=1, inplace=True)

    # postgres_hook = PostgresHook(postgres_conn_id)
    # engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention_preparation',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=False,
        start_date=datetime.today() - timedelta(days=1),
        end_date=datetime.today(),
        schedule_interval='@once'
) as dag:
    #Создает таблицы
    user_order_log_ct = PythonOperator(
        task_id='user_order_log_ct',
        python_callable=create_tables,
        op_kwargs={'query': user_order_log})

    customer_research_ct = PythonOperator(
        task_id='customer_research_ct',
        python_callable=create_tables,
        op_kwargs={'query': customer_research})

    user_activity_log_ct = PythonOperator(
        task_id='user_activity_log_ct',
        python_callable=create_tables,
        op_kwargs={'query': user_activity_log})

    # Готовим файлы
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    # Загружаем файлы основных данных
    upload_user_order_log = PythonOperator(
        task_id='upload_user_order_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    upload_customer_research = PythonOperator(
        task_id='upload_customer_research',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'customer_research.csv',
                   'pg_table': 'customer_research',
                   'pg_schema': 'staging'})

    upload_user_activity_log = PythonOperator(
        task_id='upload_user_activity_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_activity_log.csv',
                   'pg_table': 'user_activity_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}})


    (
            [user_order_log_ct, customer_research_ct, user_activity_log_ct]
            >> generate_report
            >> get_report
            >> [upload_user_order_log, upload_customer_research, upload_user_activity_log]
            >> update_d_item_table
            >> update_d_city_table
            >> update_d_customer_table
            >> update_f_sales
    )
