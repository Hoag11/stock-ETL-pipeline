from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import scripts.crawl_info
import scripts.crawl_news
import scripts.crawl_price
import scripts.crawl_report
import scripts.bot_warning
import scripts.init_db
import scripts.load_to_db

# DAG Staging
with DAG(
    dag_id="staging_dag",
    schedule_interval="30 8 * * *",  # 8h30 sáng UTC = 15h30 giờ Việt Nam
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
        python_callable=scripts.load_to_db.load_to_postgres
    )

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_transform_dag",
        trigger_dag_id="dbt_transform_dag",  # ID của DAG dbt
        wait_for_completion=True,  # Chờ DAG dbt hoàn thành trước khi tiếp tục
    )

    bot_warning_task = PythonOperator(
        task_id='bot_warning',
        python_callable=scripts.bot_warning.run
    )

    # Thứ tự thực thi
    init_db >> [crawl_info_task, crawl_news_task, crawl_price_task, crawl_report_task] >> load_task >> trigger_dbt_dag >> bot_warning_task