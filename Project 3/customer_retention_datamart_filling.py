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


#скрипт для создания витрины
f_customer_retention = '''
DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
    new_customers_count int8 NULL,
    returning_customers_count int8 NULL,
    refunded_customer_count int8 NULL,
    period_id int4 NULL,
    new_customers_revenue numeric(14, 2) NULL,
    returning_customers_revenue numeric(14, 2) NULL,
    customers_refunded int8 NULL
);
'''


insert_customer_retention = '''
insert into mart.f_customer_retention
select count(case when clients_cnt = 1 and status = 'shipped' then 1 end) as new_customers_count
    , count(case when clients_cnt > 1 and status = 'shipped' then 1 end) as returning_customers_count
    , count(case when status = 'refunded' then 1 end) as refunded_customer_count
    , cast(period_id as integer) as period_id
    , sum(case when clients_cnt = 1 and status = 'shipped' then sum_amount else 0 end) as new_customers_revenue
    , sum(case when clients_cnt > 1 and status = 'shipped' then sum_amount else 0 end) as returning_customers_revenue
    , sum(case when status = 'refunded' then sum_amount else 0 end) as customers_refunded
from (
    select period_id
        , customer_id
        , status
        , count(*) as clients_cnt
        , sum(payment_amount) as sum_amount
    from(
        select EXTRACT(YEAR FROM cast(date_id::text as date))::text || EXTRACT(WEEK FROM cast(date_id::text as date))::text as period_id
            , customer_id 
            , payment_amount 
            , case when payment_amount < 0 then 'refunded' else 'shipped' end as status
        from mart.f_sales
    ) as fs
    group by 1, 2, 3
    ) as grp
group by period_id
;
'''


def create_tables(query):
    conn.execute(query)


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention_datamart_filling',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=False,
        start_date=datetime.today() - timedelta(days=1),
        end_date=datetime.today(),
        schedule_interval='@once'
) as dag:

    # Создание датамата f_customer_retention
    f_customer_retention_ct = PythonOperator(
        task_id='f_customer_retention_ct',
        python_callable=create_tables,
        op_kwargs={'query': f_customer_retention})


    insert_customer_retention_query = PythonOperator(
        task_id='insert_customer_retention_query',
        python_callable=create_tables,
        op_kwargs={'query': insert_customer_retention})


    (
            f_customer_retention_ct
            >> insert_customer_retention_query
    )
