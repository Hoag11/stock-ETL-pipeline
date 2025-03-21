from vnstock import Vnstock
from datetime import datetime

stock = Vnstock().stock(symbol='VCI', source='VCI')
file_path = '/usr/local/airflow/data/raw_data/price'

# Lấy dữ liệu giá cổ phiếu từ 1/1/2015 đến nay
start_date = '2015-01-01'
end_date = datetime.now().strftime('%Y-%m-%d')  

symbols = ['VNM', 'HPG', 'FPT']  # Danh sách mã cổ phiếu
def run():    
    for symbol in symbols:
        df_price = stock.quote.history(symbol=symbol, start=start_date, end=end_date)
        df_price.to_csv(f'{file_path}/{symbol}_price.csv', index=False)
        print(f"Đã lưu dữ liệu cho {symbol}")
