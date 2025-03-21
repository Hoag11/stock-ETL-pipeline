import pandas as pd
from vnstock3 import Vnstock

def run():
    symbols = ['HPG', 'FPT', 'VNM']
    base_path = '/usr/local/airflow/data/raw_data/company_info'

    for symbol in symbols:
        company = Vnstock().stock(symbol=symbol, source='TCBS').company
        df = company.profile()
        df.to_csv(f'{base_path}/{symbol}_profile.csv', index=False)
        print(f"Đã lưu thông tin công ty {symbol}.")

if __name__ == "__main__":
    run()