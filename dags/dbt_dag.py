# dags/dbt_dag.py
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime

# Cấu hình profile cho dbt
profile_config = ProfileConfig(
    profile_name="stockwh_01",  
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_stock_db",
        profile_args={"schema": "wh"},
    ),
)

# DAG dbt
dbt_dag = DbtDag(
    dag_id="dbt_dag",
    schedule_interval="3 8 * * *",
    start_date=datetime(2025, 3, 20, 8, 0),
    catchup=False,
    default_args={"retries": 2},
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dags/stockwh_01",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
)