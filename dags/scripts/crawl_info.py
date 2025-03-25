import pandas as pd
from vnstock3 import Vnstock
import scripts.config as config
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def process_symbol(symbol, base_path, rate_limit):
    company = Vnstock().stock(symbol=symbol, source='TCBS').company
    df = company.profile()
    df.to_csv(f'{base_path}/{symbol}_profile.csv', index=False)
    print(f"Đã lưu thông tin công ty {symbol}.")
    time.sleep(rate_limit)

def run():
    rate_limit = config.rate_limit
    symbols = config.all_symbols
    base_path = '/usr/local/airflow/data/raw_data/company_info'
    max_workers = config.max_workers

    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_symbol, symbol, base_path, rate_limit) for symbol in symbols]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    run()