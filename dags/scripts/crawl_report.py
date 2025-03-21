from vnstock import *

file_path = '/usr/local/airflow/data/raw_data/company_report'
stock_id = ['HPG', 'FPT', 'VNM']
def run():    
    for i in stock_id:
        stock = Vnstock().stock(symbol=i, source='TCBS')
        df_income = stock.finance.income_statement(period='quarter')
        df_balance = stock.finance.balance_sheet(period='quarter')
        df_cash = stock.finance.cash_flow(period='quarter')
        df_income.to_csv(f'{file_path}/{i}_income.csv')
        df_balance.to_csv(f'{file_path}/{i}_balance.csv') 
        df_cash.to_csv(f'{file_path}/{i}_cash.csv')
        print(f"Đã lưu dữ liệu cho {i}")
