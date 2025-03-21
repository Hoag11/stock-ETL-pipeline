from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import scripts.crawl_info
import scripts.crawl_news
import scripts.crawl_price
import scripts.crawl_report
import scripts.bot_warning
import scripts.init_db
import pandas as pd
from sqlalchemy import create_engine

# Load dữ liệu vào PostgreSQL
def load_to_postgres():
    engine = create_engine('postgresql://postgres:postgres@stock_b7156c-postgres-1/stockwh')
    symbols = ['VNM', 'HPG', 'FPT']
    base_path = '/usr/local/airflow/data/raw_data'

    for symbol in symbols:
        # Load price
        df_price = pd.read_csv(f'{base_path}/price/{symbol}_price.csv')
        df_price['symbol'] = symbol
        df_price.to_sql('price', engine, schema='raw', if_exists='append', index=False)

        # Load profile
        df_profile = pd.read_csv(f'{base_path}/company_info/{symbol}_profile.csv')
        df_profile['symbol'] = symbol
        df_profile.to_sql('profile', engine, schema='raw', if_exists='append', index=False)

        # Load news
        df_news = pd.read_csv(f'{base_path}/company_news/{symbol}_news.csv')
        df_news['symbol'] = symbol
        df_news.to_sql('news', engine, schema='raw', if_exists='append', index=False)

        # Load report
        df_income = pd.read_csv(f'{base_path}/company_report/{symbol}_income.csv')
        df_income['symbol'] = symbol
        df_income.to_sql('income', engine, schema='raw', if_exists='append', index=False)

        df_balance = pd.read_csv(f'{base_path}/company_report/{symbol}_balance.csv')
        df_balance['symbol'] = symbol
        df_balance.to_sql('balance', engine, schema='raw', if_exists='append', index=False)

        df_cash = pd.read_csv(f'{base_path}/company_report/{symbol}_cash.csv')
        df_cash['symbol'] = symbol
        df_cash.to_sql('cash', engine, schema='raw', if_exists='append', index=False)

# DAG Staging
with DAG(
    dag_id="staging_dag",
    schedule_interval="0 8 * * *",
    start_date=datetime(2025, 3, 20, 8, 0),
    catchup=False,
    default_args={"retries": 3},
) as dag:
    init_db = PythonOperator(
        task_id='init_db',
        python_callable=scripts.init_db.init_db
    )

    crawl_info_task = PythonOperator(
        task_id='crawl_info',
        python_callable=scripts.crawl_info.run
    )

    crawl_news_task = PythonOperator(
        task_id='crawl_news',
        python_callable=scripts.crawl_news.run
    )

    crawl_price_task = PythonOperator(
        task_id='crawl_price',
        python_callable=scripts.crawl_price.run
    )

    crawl_report_task = PythonOperator(
        task_id='crawl_report',
        python_callable=scripts.crawl_report.run
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    bot_warning_task = PythonOperator(
        task_id='bot_warning',
        python_callable=scripts.bot_warning.run
    )

    # Thứ tự thực thi
    init_db >> crawl_info_task >> crawl_news_task >> crawl_price_task >> crawl_report_task >> load_task >> bot_warning_task