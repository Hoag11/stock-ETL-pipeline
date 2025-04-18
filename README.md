![alt text](image.png)

# StockWH Project

## Project Description

**StockWH** is a data warehouse project aimed at collecting, processing, and analyzing stock data from listed companies in Vietnam (e.g., HPG, FPT, LPB). The project uses Airflow to manage the ETL (Extract, Transform, Load) pipeline and dbt to transform raw data into dimension and fact tables, enabling comprehensive financial and stock price analyses.

Link to Telegram alarm price (VNM, FPT, HPG): https://t.me/+_EnUuDnldM00ZGU9

### Objectives
- Collect raw data from various sources such as stock prices, news articles, financial statements (income, balance, cash flow).
- Store raw data in the raw schema of the stockwh database.
- Transform raw data into dimension tables (`dim_company`) and fact table (`fact_price`) within the wh schema of the stockwh database.
- Provide processed data for financial analyses, such as revenue, profit, and the impact of news on stock prices.

### Technologies
- **Airflow**: Manage ETL pipelines.
- **dbt**: Data transformation.
- **PostgreSQL**: Data storage (`stockwh` database).
- **Python**: Scripting for crawling and loading data (using the `vnstock3` library).
- **Docker**: Running Airflow and PostgreSQL in containers.

---

## System Requirements

- **Docker** và **Docker Compose**: To run Airflow and PostgreSQL.
- **Python 3.10**: For running data crawling and loading scripts.
- **dbt 1.6.0**: Data transformation (compatible with `Cosmos`).
- **PostgreSQL**: The `stockwh` database for data storage.
- **Astro CLI**: For running the project.

---
