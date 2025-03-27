from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql

# Hàm kiểm tra và tạo database nếu chưa tồn tại
def create_database():
    try:
        # Kết nối đến PostgreSQL server (không cần chỉ định database)
        conn = psycopg2.connect(
            host="stock_b7156c-postgres-1",
            port=5432,
            user="postgres",
            password="postgres",
            database="postgres" 
        )
        conn.autocommit = True  # Để tạo database, cần bật autocommit
        cursor = conn.cursor()

        # Kiểm tra xem database stockwh đã tồn tại chưa
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'stockwh'")
        exists = cursor.fetchone()
        if not exists:
            # Tạo database stockwh nếu chưa tồn tại
            cursor.execute(sql.SQL("CREATE DATABASE stockwh"))
            print("Database stockwh created successfully.")
        else:
            print("Database stockwh already exists.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'init_db_dag',
    default_args=default_args,
    description='DAG to initialize the stockwh database, schemas, and tables',
    schedule_interval="@once",  # Chỉ chạy một lần khi Airflow khởi động
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Tạo database stockwh
    create_db_task = PythonOperator(
        task_id='create_database',
        python_callable=create_database,
    )

    # Task 2: Tạo schema raw và các bảng trong raw
    create_raw_schema_task = PostgresOperator(
        task_id='create_raw_schema',
        postgres_conn_id='postgres_stock_db',
        sql='/usr/local/airflow/dags/scripts/sql/create_raw_schema.sql',
        database='stockwh',
    )

    # Task 3: Tạo schema wh và các bảng trong wh
    create_wh_schema_task = PostgresOperator(
        task_id='create_wh_schema',
        postgres_conn_id='postgres_stock_db',
        sql='/usr/local/airflow/dags/scripts/sql/create_wh_schema.sql',
        database='stockwh',
    )

    # Định nghĩa thứ tự chạy
    create_db_task >> create_raw_schema_task >> create_wh_schema_task