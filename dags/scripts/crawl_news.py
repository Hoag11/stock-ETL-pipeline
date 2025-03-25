from vnstock import Vnstock
from concurrent.futures import ThreadPoolExecutor, as_completed
import scripts.config as config
import time

file_path = '/usr/local/airflow/data/raw_data/company_news'
stock_id = config.all_symbols

def process_news(symbol, file_path, rate_limit):
    company = Vnstock().stock(symbol=symbol, source='TCBS').company
    df_news = company.news()
    df_news.to_csv(f'{file_path}/{symbol}_news.csv', index=False)
    print(f"Đã lưu dữ liệu cho {symbol}")
    time.sleep(rate_limit)

def run():
    max_workers = config.max_workers
    rate_limit = config.rate_limit
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_news, symbol, file_path, rate_limit) for symbol in stock_id]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    run()