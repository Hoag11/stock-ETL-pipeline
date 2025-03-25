from vnstock import *
import scripts.config as config
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


file_path = '/usr/local/airflow/data/raw_data/company_report'
stock_id = config.all_symbols

def process_report(symbol, file_path, rate_limit):
    stock = Vnstock().stock(symbol=symbol, source='TCBS')
    df_income = stock.finance.income_statement(period='quarter')
    df_balance = stock.finance.balance_sheet(period='quarter')
    df_cash = stock.finance.cash_flow(period='quarter')
    df_income.to_csv(f'{file_path}/{symbol}_income.csv')
    df_balance.to_csv(f'{file_path}/{symbol}_balance.csv') 
    df_cash.to_csv(f'{file_path}/{symbol}_cash.csv')
    print(f"Đã lưu dữ liệu cho {symbol}")
    time.sleep(rate_limit)

def run():    
    max_workers = config.max_workers
    rate_limit = config.rate_limit
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_report, symbol, file_path, rate_limit) for symbol in stock_id]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    run()