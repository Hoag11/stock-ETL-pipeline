from vnstock import Vnstock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import scripts.config as config
import time

file_path = '/usr/local/airflow/data/raw_data/price'
start_date = '2015-01-01'
end_date = datetime.now().strftime('%Y-%m-%d')  
symbols = config.all_symbols

def process_price(symbol, file_path, start_date, end_date, rate_limit):
    stock = Vnstock().stock(symbol=symbol, source='TCBS')
    df_price = stock.quote.history(symbol=symbol, start=start_date, end=end_date)
    df_price.to_csv(f'{file_path}/{symbol}_price.csv', index=False)
    print(f"Đã lưu dữ liệu cho {symbol}")
    time.sleep(rate_limit)

def run():
    max_workers = config.max_workers
    rate_limit = config.rate_limit
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_price, symbol, file_path, start_date, end_date, rate_limit) for symbol in symbols]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    run()