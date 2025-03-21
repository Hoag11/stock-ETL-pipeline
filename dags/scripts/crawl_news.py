from vnstock import *

file_path = '/usr/local/airflow/data/raw_data/company_news'

stock_id = ['HPG', 'FPT', 'VNM']
def run():
    for i in stock_id:
        company = Vnstock().stock(symbol=i, source='TCBS').company
        df_news = company.news()
        df_news.to_csv(f'{file_path}/{i}_news.csv', index=False)
        print(f"Đã lưu dữ liệu cho {i}")

        